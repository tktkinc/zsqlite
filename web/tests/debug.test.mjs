import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { WebSocket } from 'ws';
import { attachDebug } from '../debug-server.mjs';

test('debug websocket records browser failures and dispatches bounded commands only to its own session', async () => {
  let hub;
  const server = createServer((req,res) => { void hub.handle(req,res,new URL(req.url,'http://localhost').pathname); });
  hub = attachDebug(server);
  await new Promise(resolve => server.listen(0,'127.0.0.1',resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  const ws = new WebSocket(base.replace('http:','ws:')+'/debug/ws',{origin:base});
  const messages = [];
  ws.on('message',raw=>messages.push(JSON.parse(raw.toString())));
  try {
    await once(ws,'open');
    await new Promise(resolve=>setTimeout(resolve,20));
    const id = messages[0].id;
    ws.send(JSON.stringify({kind:'hello',info:{userAgent:'mobile test'}}));
    ws.send(JSON.stringify({kind:'event',name:'page-error',data:{message:'worker startup failed'}}));
    await new Promise(resolve=>setTimeout(resolve,20));
    const headers = {Authorization:`Bearer ${hub.token}`};
    assert.equal((await fetch(base+'/api/debug/sessions')).status,401);
    const list=await (await fetch(base+'/api/debug/sessions',{headers})).json();
    assert.equal(list[0].info.userAgent,'mobile test');
    const events=await (await fetch(`${base}/api/debug/sessions/${id}/events`,{headers})).json();
    assert.ok(events.some(event=>event.kind==='page-error'&&event.data.message==='worker startup failed'));
    const command=fetch(`${base}/api/debug/sessions/${id}/command`,{method:'POST',headers,body:JSON.stringify({action:'snapshot'})});
    const [raw]=await once(ws,'message');
    const request=JSON.parse(raw.toString());
    assert.equal(request.action,'snapshot');
    ws.send(JSON.stringify({kind:'result',id:request.id,result:{connected:false}}));
    assert.deepEqual(await (await command).json(),{result:{connected:false}});
    assert.equal((await fetch(`${base}/api/debug/sessions/${id}/command`,{method:'POST',headers,body:JSON.stringify({action:'eval',code:'arbitrary code'})})).status,400);
    const hostile=new WebSocket(base.replace('http:','ws:')+'/debug/ws',{origin:'https://another.example'});
    const [response]=await once(hostile,'unexpected-response').then(([,res])=>[res]);
    assert.equal(response.statusCode,403);hostile.terminate();hostile.on('error',()=>{});
  } finally { ws.terminate();hub.close();server.closeAllConnections();await new Promise(resolve=>server.close(resolve)); }
});
