import asyncio
import json
from pathlib import Path
from types import SimpleNamespace as NS
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from astrbot.core.workspace_runtime import binding, execution_key, process_key, authorize_tool, refuse_busy_reload
from astrbot.core.pipeline.process_stage.follow_up_guard import binding_guard_allows
from astrbot_plugin_task_workspace.models import Workspace
from astrbot_plugin_task_workspace.store import Store, MIGRATIONS
from astrbot_plugin_task_workspace.native import NativeLedger, NativeService
from astrbot_plugin_task_workspace.window_policy import LINUX_TOOLS, sanitize_dsml
from astrbot_plugin_task_workspace.config import TaskWorkspaceConfig
from astrbot_plugin_task_workspace.manager import WorkspaceManager, WorkspaceError
from astrbot_plugin_task_workspace.memory_gate import MemoryGate
from astrbot_plugin_task_workspace.exec_adapter import FakeRemoteExecutor
from astrbot_plugin_task_workspace.binding import BindingPolicy, dual_runtime_ready

class Event:
    def __init__(self, ws=None, text='检查目录', sender='admin'):
        self.unified_msg_origin = 'p:FriendMessage:1'
        self.role = 'admin'
        self.extras = {}
        self.text = text
        self.sender = sender
        if ws:
            self.extras['_task_workspace_binding'] = {
                'managed': True, 'umo': ws.umo, 'owner': ws.owner_user_id,
                'cid': ws.conversation_id, 'ws_id': ws.id, 'environment': ws.environment,
                'cwd': ws.cwd, 'title': ws.title, 'allowed_tools': list(LINUX_TOOLS) if ws.environment == 'linux_local' else [],
            }
    def get_extra(self, name, default=None): return self.extras.get(name, default)
    def set_extra(self, name, value): self.extras[name] = value
    def get_sender_id(self): return self.sender
    def is_admin(self): return self.role == 'admin'
    def is_private_chat(self): return True
    def get_message_str(self): return self.text
    def get_platform_id(self): return 'p'


def workspace(id='ws_linux1', env='linux_local', cid='linux-cid'):
    return Workspace(id=id, owner_user_id='admin', umo='p:FriendMessage:1',
                     conversation_id=cid, title='same title', cwd='/tmp', environment=env)

class RuntimeTests(unittest.TestCase):
    def test_keys_and_frozen_environment(self):
        linux = Event(workspace())
        windows = Event(workspace('ws_windows', 'windows_remote', 'win-cid'))
        other = Event(workspace('ws_linux2', 'linux_local', 'linux-cid2'))
        self.assertEqual(len({execution_key(e) for e in [linux, windows, other]}), 3)
        self.assertFalse(binding_guard_allows(windows, linux))
        self.assertTrue(binding_guard_allows(Event(workspace()), linux))
        self.assertEqual(execution_key(Event()), 'p:FriendMessage:1')
        linux.set_extra('_task_workspace_native_id', 'local_abc')
        self.assertTrue(process_key(linux).endswith(':local_abc'))
        self.assertTrue(dual_runtime_ready())

    def test_invalid_binding_never_falls_back(self):
        event = Event(workspace(), sender='intruder')
        with self.assertRaises(RuntimeError): execution_key(event)

    def test_dsml_false_execution_and_documentation(self):
        dsml = '<｜｜DSML｜｜ calls><｜｜DSML｜｜ invoke name="astrbot_execute_shell">'
        clean, blocked = sanitize_dsml('我实际执行一下目录列表。' + dsml, dsml + '这些是什么', 'windows_remote')
        self.assertTrue(blocked)
        self.assertIn('未执行', clean)
        self.assertNotIn('DSML', clean)
        self.assertFalse(sanitize_dsml('这是语法示例：' + dsml, '解释 DSML 格式', 'linux_local')[1])
        self.assertFalse(sanitize_dsml('普通结果', '检查目录', 'linux_local')[1])

def sandbox_loop():
    loop = asyncio.new_event_loop()
    # Restricted Unix sockets can prevent worker-thread wakeups in the test
    # sandbox. A timer keeps SQLite to_thread completions observable.
    def tick():
        loop.call_later(0.02, tick)
    loop.call_later(0.02, tick)
    return loop

class StoreAndServiceTests(unittest.IsolatedAsyncioTestCase):
    loop_factory = staticmethod(sandbox_loop)
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / 'db.sqlite')
        await self.store.migrate()
        self.ws = workspace()
        self.win = workspace('ws_windows', 'windows_remote', 'win-cid')
        await self.store.insert_workspace(self.ws)
        await self.store.insert_workspace(self.win)
        self.cfg = TaskWorkspaceConfig({'enabled':True,'allowed_sessions':['p:FriendMessage:1'],
                                        'default_permission':'workspace-write','require_native_guard':True})
        self.cm = NS(get_curr_conversation_id=AsyncMock(return_value='linux-cid'),
                     get_conversation=AsyncMock(return_value=NS(history='[]',cid='linux-cid',user_id=self.ws.umo)))
        self.manager = WorkspaceManager(config=self.cfg, store=self.store,
            executor=FakeRemoteExecutor(), notifier=NS(enqueue=AsyncMock()), cm=self.cm,
            memory_gate=MemoryGate(capability_probe=lambda: True), results_dir=Path(self.tmp.name), sp=None)
        self.context = NS(get_config=lambda **kw: {'provider_settings':{'computer_use_runtime':'local'}})
        self.native = NativeService(self.manager, self.context)
        self.context._task_workspace_service = NS(available=True,native=self.native)
        self.native.sessions = AsyncMock(return_value={'sessions':[]})

    async def asyncTearDown(self):
        await self.native.close()
        await self.manager.shutdown()
        self.store.close()
        self.tmp.cleanup()

    async def test_migration_idempotent_and_environment_immutable(self):
        self.assertEqual(await self.store.migrate(), 3)
        got = await self.store.get_workspace(self.ws.id)
        self.assertEqual(got.environment, 'linux_local')
        with self.assertRaises(ValueError): await self.store.update_workspace(self.ws.id, environment='windows_remote')

    async def test_upgrade_existing_v2_keeps_history(self):
        old = Store(Path(self.tmp.name) / 'old.sqlite')
        with patch('astrbot_plugin_task_workspace.store.MIGRATIONS', MIGRATIONS[:2]):
            await old.migrate()
        c=old._connect()
        c.execute("INSERT INTO workspaces(id,id_prefix,owner_user_id,umo,conversation_id,title,created_ts,updated_ts) VALUES('old','old','admin','u','c','legacy',1,1)")
        await old.migrate()
        got=await old.get_workspace('old')
        self.assertEqual((got.title,got.environment),('legacy','windows_remote'))
        old.close()

    async def test_true_tool_gate_after_window_switch(self):
        event=Event(self.ws)
        await self.native.begin(event, execution_key(event))
        self.cm.get_curr_conversation_id.return_value='win-cid'
        await authorize_tool(self.context,event,'astrbot_execute_shell')
        with self.assertRaises(RuntimeError):
            await authorize_tool(self.context,Event(self.win),'astrbot_execute_shell')
        with self.assertRaises(RuntimeError):
            await authorize_tool(self.context,event,'unlisted_tool')
        await self.native.finish(event, NS(completion_text='done'))
        with self.assertRaises(RuntimeError):
            await authorize_tool(self.context,event,'astrbot_execute_shell')

    async def test_missing_binding_in_managed_window_denied(self):
        policy=BindingPolicy();policy.load([self.ws]);policy.set_current(self.ws.umo,self.ws.conversation_id)
        self.context._task_workspace_binding_policy=policy
        with self.assertRaises(RuntimeError): await authorize_tool(self.context,Event(),'astrbot_execute_shell')

    async def test_atomic_limits_and_restart_no_replay(self):
        ledger=self.native.ledger
        ids=await asyncio.gather(ledger.reserve(self.ws,'scope',2),ledger.reserve(self.ws,'scope',2),return_exceptions=True)
        self.assertEqual(sum(isinstance(x, str) for x in ids),1)
        ws2=workspace('ws_second','linux_local','c2');await self.store.insert_workspace(ws2)
        await ledger.reserve(ws2,'scope2',2)
        with self.assertRaises(RuntimeError): await ledger.reserve(workspace('ws_third','linux_local','c3'),'scope3',2)
        await self.native.initialize()
        rows=await ledger.query('SELECT status FROM native_executions')
        self.assertEqual({r['status'] for r in rows},{'interrupted'})

    async def test_cancel_by_id_after_switch_and_ownership(self):
        event=Event(self.ws);await self.native.begin(event,execution_key(event))
        rid=event.get_extra('_task_workspace_native_id')
        runner=NS(request_stop=lambda:event.set_extra('requested',True));self.native.register(event,runner)
        self.native._stop_sessions=AsyncMock()
        self.cm.get_curr_conversation_id.return_value='win-cid'
        result=await self.native.command(Event(self.win),'cancel',rid[:14])
        self.assertIn('已请求',result)
        self.assertTrue(event.get_extra('requested'))
        await self.native.finish(event,NS(completion_text='stopped'))
        status=(await self.native.ledger.query('SELECT status FROM native_executions WHERE id=?',(rid,)))[0]['status']
        self.assertEqual(status,'cancelled')
        self.assertIn('无权',await self.native.command(Event(self.win,sender='other'),'result',rid))

    async def test_remote_command_in_linux_window_never_submits(self):
        with self.assertRaisesRegex(WorkspaceError,'Linux'):
            await self.manager.start_run(Event(self.ws),'codex','write a file')
        self.assertEqual(await self.store.active_runs(),[])

    async def test_busy_reload_blocked_before_teardown(self):
        e=Event(self.ws);await self.native.begin(e,execution_key(e))
        with self.assertRaisesRegex(RuntimeError,'活跃'):
            await refuse_busy_reload(self.context,None)
        await self.native.finish(e,NS(completion_text='done'))
        await refuse_busy_reload(self.context,None)

    async def test_sanitize_assistant_history_not_user_quotation(self):
        from astrbot.core.agent.message import Message,TextPart
        e=Event(self.win)
        bad='<｜｜DSML｜｜ calls><｜｜DSML｜｜ invoke name="astrbot_execute_shell">'
        messages=[Message(role='assistant',content=[TextPart(text=bad)]),Message(role='user',content=bad)]
        response=NS(completion_text=bad,result_chain=None)
        self.native.sanitize(e,response,messages)
        self.assertIn('未执行',response.completion_text)
        self.assertIn('未执行',messages[0].content[0].text)
        self.assertEqual(messages[1].content,bad)

    async def test_actual_frozen_conversation_and_cwd(self):
        from astrbot.core.astr_main_agent import _get_session_conv
        from astrbot.core.tools.computer_tools.util import workspace_root_for_context
        self.context.conversation_manager=self.cm
        event=Event(self.ws)
        self.cm.get_curr_conversation_id.return_value='win-cid'
        await _get_session_conv(event,self.context)
        self.cm.get_conversation.assert_awaited_with(self.ws.umo,'linux-cid')
        path=await workspace_root_for_context(NS(context=NS(event=event,context=self.context)))
        self.assertEqual(str(path),'/tmp')

    async def test_main_and_file_tools_import(self):
        from astrbot_plugin_task_workspace.main import TaskWorkspace
        from astrbot_plugin_task_workspace.local_files import LinuxDownloadTool,LinuxSendFileTool
        from astrbot.core.agent.tool import ToolSet
        toolset=ToolSet();toolset.add_tool(LinuxDownloadTool());toolset.add_tool(LinuxSendFileTool())
        self.assertEqual(set(toolset.names()),{'linux_download_file','linux_send_file'})
        self.assertIn('/task new linux',TaskWorkspace._HELP_TEXT)

    async def test_window_hook_exposes_only_local_tools(self):
        from astrbot_plugin_task_workspace.main import TaskWorkspace
        from astrbot.core.agent.tool import ToolSet
        from astrbot.core.tools.computer_tools.shell import LocalExecuteShellTool
        plugin=object.__new__(TaskWorkspace)
        plugin._cfg=self.cfg;plugin._manager=self.manager;plugin.native=self.native
        for ws in (self.ws,self.win):
            event=Event(ws)
            event.stop_event=lambda: self.fail('valid event stopped')
            tools=ToolSet();tools.add_tool(LocalExecuteShellTool())
            req=NS(func_tool=tools,system_prompt='',conversation=NS(cid=ws.conversation_id),contexts=[])
            await plugin._surface_result(event,req)
            if ws.environment=='linux_local':
                self.assertIn('astrbot_execute_shell',req.func_tool.names())
                self.assertIn('linux_send_file',req.func_tool.names())
                self.assertIn('Linux',req.system_prompt)
            else:
                self.assertEqual(req.func_tool.names(),[])
                self.assertIn('/task run',req.system_prompt)

    async def test_remote_permission_is_snapshotted(self):
        self.cm.get_curr_conversation_id.return_value='win-cid'
        self.manager._spawn=lambda coro:coro.close()
        run=await self.manager.start_run(Event(self.win),'codex','write only in workspace')
        self.assertEqual(json.loads(run.input_snapshot)['permission'],'workspace-write')
        self.cfg._cfg['default_permission']='read-only'
        self.assertEqual(json.loads((await self.store.get_run(run.id)).input_snapshot)['permission'],'workspace-write')

    async def test_actual_shell_sessions_scoped_and_cancellable(self):
        from astrbot.core.computer.booters.local import LocalShellComponent
        shell=LocalShellComponent()
        try:
            a=await shell.exec_managed("printf window-a; sleep 20",owner_id='window-a',creator_id='admin',creator_is_admin=True,sandboxed=False,cwd=self.tmp.name,yield_time_ms=0,timeout=30)
            b=await shell.exec_managed("printf window-b; sleep 20",owner_id='window-b',creator_id='admin',creator_is_admin=True,sandboxed=False,cwd=self.tmp.name,yield_time_ms=0,timeout=30)
            with self.assertRaises(ValueError):
                await shell.terminate_session(owner_id='window-b',requester_id='admin',requester_is_admin=True,session_id=a['session_id'])
            await asyncio.sleep(.05)
            snap=await shell.snapshot_session(owner_id='window-a',requester_id='admin',requester_is_admin=True,session_id=a['session_id'])
            self.assertIn('window-a',snap['stdout'])
            await shell.terminate_session(owner_id='window-a',requester_id='admin',requester_is_admin=True,session_id=a['session_id'])
            items=(await shell.list_sessions(owner_id='window-b',requester_id='admin',requester_is_admin=True))['sessions']
            self.assertEqual(items[0]['status'],'running')
        finally:
            await shell.shutdown_sessions()

    async def test_background_ledger_waits_for_registered_process(self):
        e=Event(self.ws)
        await self.native.begin(e,execution_key(e))
        self.native.sessions.return_value={'sessions':[{'session_id':'s1','status':'running'}]}
        await self.native.finish(e,NS(completion_text='started'))
        rows=await self.native.ledger.query('SELECT * FROM native_executions')
        self.assertEqual(rows[0]['status'],'background')
        self.assertTrue(await self.native.busy())
        self.native._session_output=AsyncMock(return_value='background output')
        self.native.sessions.return_value={'sessions':[{'session_id':'s1','status':'completed'}]}
        watcher=asyncio.create_task(self.native._watch_background())
        try:
            await asyncio.sleep(2.1)
            rows=await self.native.ledger.query('SELECT * FROM native_executions')
            self.assertEqual(rows[0]['status'],'succeeded')
            self.assertIn('background output',rows[0]['result_text'])
        finally:
            watcher.cancel();await asyncio.gather(watcher,return_exceptions=True)

    async def test_core_stage_cross_window_and_buffered_reply(self):
        from dataclasses import dataclass
        from astrbot.core.pipeline.process_stage.method.agent_sub_stages import internal
        from astrbot.core.provider.entities import LLMResponse
        from astrbot.core.agent.message import Message,TextPart
        from astrbot.core.agent.tool import ToolSet
        @dataclass
        class BuildCfg:
            provider_wake_prefix: str=''
            streaming_response: bool=True
        stage=internal.InternalAgentSubStage()
        stage.streaming_response=True;stage.unsupported_streaming_strategy='turn_off'
        stage.max_step=5;stage.show_tool_use=False;stage.show_tool_call_result=False
        stage.show_reasoning=False;stage.buffer_intermediate_messages=True
        stage.main_agent_cfg=BuildCfg()
        stage.ctx=NS(plugin_manager=NS(context=self.context))
        saved=[]
        async def save(event,req,resp,*args,**kwargs): saved.append((req.conversation.cid,resp.completion_text))
        stage._save_to_history=save
        started=asyncio.Event();release=asyncio.Event()
        def fixture_event(ws):
            e=Event(ws);e.message_str=e.text;e.platform_meta=NS(support_streaming_message=True)
            e.message_obj=NS(message=[],message_id=ws.id);e.trace=NS(record=lambda *a,**k:None)
            e.send_typing=AsyncMock();e.stop_typing=AsyncMock();e.send=AsyncMock()
            e.set_result=lambda r:e.set_extra('result',r)
            e.clear_result=lambda:e.set_extra('result',None)
            e.is_stopped=lambda:False
            return e
        async def build(**kw):
            e=kw['event'];cid=binding(e)['cid']
            self.assertFalse(kw['config'].streaming_response)
            response=LLMResponse(role='assistant',completion_text='<｜｜DSML｜｜ calls>')
            runner=NS(run_context=NS(context=NS(event=e),messages=[]),
                      stats=NS(to_dict=lambda:{}),provider=NS(get_model=lambda:'fake',meta=lambda:NS(type='fake')),
                      get_final_llm_resp=lambda:response,was_aborted=lambda:False,request_stop=lambda:None)
            return NS(agent_runner=runner,provider_request=NS(conversation=NS(cid=cid),system_prompt='',func_tool=ToolSet()),provider=NS(provider_config={},get_model=lambda:'fake',meta=lambda:NS(type='fake')),reset_coro=None)
        async def fake_run(runner,*args,**kwargs):
            e=runner.run_context.context.event
            if binding(e)['environment']=='linux_local':
                started.set();await release.wait()
            runner.run_context.messages.append(Message(role='assistant',content=[TextPart(text=runner.get_final_llm_resp().completion_text)]))
            e.set_result(NS(leaked='raw partial'))
            yield 'raw partial'
        async def consume(e):
            out=[]
            async for _ in stage.process(e,''):
                out.append(e.get_extra('result'))
            return out
        with patch.object(internal,'build_main_agent',side_effect=build),patch.object(internal,'run_agent',side_effect=fake_run),patch.object(internal,'call_event_hook',new=AsyncMock(return_value=False)),patch.object(internal,'_record_internal_agent_stats',new=AsyncMock()),patch.object(internal.Metric,'upload',new=AsyncMock()):
            linux=fixture_event(self.ws);windows=fixture_event(self.win)
            task=asyncio.create_task(consume(linux))
            try:
                await asyncio.wait_for(started.wait(),2)
                self.cm.get_curr_conversation_id.return_value='win-cid'
                win_results=await asyncio.wait_for(consume(windows),2)
                self.assertFalse(task.done())
                self.assertEqual(len(win_results),1)
                release.set()
                linux_results=await asyncio.wait_for(task,2)
                self.assertEqual(len(linux_results),1)
                linux.send.assert_not_awaited();windows.send.assert_not_awaited()
                self.assertEqual({cid for cid,_ in saved},{'linux-cid','win-cid'})
                self.assertTrue(all('未执行' in text and '<｜｜DSML' not in text for _,text in saved))
            finally:
                release.set()
                await asyncio.gather(task,return_exceptions=True)

    async def test_archive_and_cancel_completion_race(self):
        e=Event(self.ws);await self.native.begin(e,execution_key(e))
        with self.assertRaises(ValueError):
            await self.store.update_workspace(self.ws.id,archived=1)
        rid=e.get_extra('_task_workspace_native_id')
        self.native._stop_sessions=AsyncMock()
        # Serialize cancellation before completion; completion must retain it.
        await self.native.command(e,'cancel',rid)
        await self.native.finish(e,NS(completion_text='late response'))
        row=(await self.native.ledger.query('SELECT status FROM native_executions WHERE id=?',(rid,)))[0]
        self.assertEqual(row['status'],'cancelled')
        await self.store.update_workspace(self.ws.id,archived=1)
        with self.assertRaises(RuntimeError): await self.native.ledger.reserve(self.ws,'scope',2)

    async def test_actual_tool_dispatch_cannot_bypass_windows_gate(self):
        from astrbot.core.astr_agent_tool_exec import FunctionToolExecutor
        tool=NS(name='astrbot_execute_shell',call=AsyncMock())
        run_context=NS(context=NS(context=self.context,event=Event(self.win)))
        with self.assertRaises(RuntimeError):
            async for _ in FunctionToolExecutor.execute(tool,run_context,command='should never run'):
                pass
        tool.call.assert_not_awaited()

    async def test_builtin_stop_does_not_cancel_other_window(self):
        from astrbot.core.utils.active_event_registry import ActiveEventRegistry
        registry=ActiveEventRegistry()
        linux=Event(self.ws);windows=Event(self.win)
        linux.stop_event=lambda:linux.set_extra('stopped',True)
        windows.stop_event=lambda:windows.set_extra('stopped',True)
        registry.register(linux);registry.register(windows)
        initiating_command=Event(self.win)
        self.assertEqual(registry.stop_all(self.win.umo,exclude=initiating_command),1)
        self.assertTrue(windows.get_extra('stopped'))
        self.assertFalse(linux.get_extra('stopped'))

    async def test_last_hook_filter_rejects_late_tools_and_wrong_conversation(self):
        from astrbot.core.agent.tool import ToolSet
        from astrbot.core.tools.computer_tools.shell import LocalExecuteShellTool
        e=Event(self.win)
        tools=ToolSet();tools.add_tool(LocalExecuteShellTool())
        req=NS(conversation=NS(cid='win-cid'),func_tool=tools,system_prompt='')
        await self.native.restrict_request(e,req)
        self.assertEqual(req.func_tool.names(),[])
        req.conversation.cid='linux-cid'
        with self.assertRaisesRegex(RuntimeError,'不一致'):
            await self.native.restrict_request(e,req)

    async def test_linux_does_not_fall_back_to_sandbox(self):
        self.context.get_config=lambda **kw:{'provider_settings':{'computer_use_runtime':'sandbox'}}
        with self.assertRaisesRegex(RuntimeError,'local'):
            await self.native.begin(Event(self.ws),execution_key(Event(self.ws)))
        self.assertEqual(await self.native.ledger.query('SELECT * FROM native_executions'),[])

    async def test_explicit_remote_id_routes_from_linux_without_switching(self):
        self.cm.get_curr_conversation_id.return_value='win-cid'
        self.manager._spawn=lambda coro:coro.close()
        run=await self.manager.start_run(Event(self.win),'codex','a remote task')
        self.cm.get_curr_conversation_id.return_value='linux-cid'
        self.assertIsNone(await self.native.command(Event(self.ws),'status',run.id))
        got=await self.manager.status(Event(self.ws),run.id)
        self.assertEqual(got.id,run.id)
        cancelled=await self.manager.cancel(Event(self.ws),run.id)
        self.assertEqual(cancelled.status,'cancel_requested')
        with self.assertRaises(WorkspaceError):
            await self.manager.status(Event(self.ws,sender='other'),run.id)
        self.assertEqual(self.cm.get_curr_conversation_id.return_value,'linux-cid')
