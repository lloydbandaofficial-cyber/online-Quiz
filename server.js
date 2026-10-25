const http=require("http"),fs=require("fs"),path=require("path"),{WebSocketServer}=require("ws");
const html=fs.readFileSync(path.join(__dirname,"index.html"));
const server=http.createServer((q,s)=>{
  if(q.url==="/health"){s.end("ok");return}
  s.writeHead(200,{"Content-Type":"text/html; charset=utf-8"});s.end(html);
});
const wss=new WebSocketServer({server});
const rooms=new Map();let n=0;
function snap(code){
  const r=rooms.get(code);if(!r)return;
  const peers=[...r.values()].map(c=>({id:c.id,p:c.p}));
  for(const [ws,c] of r)if(ws.readyState===1)ws.send(JSON.stringify({t:"s",me:c.id,peers}));
}
wss.on("connection",ws=>{
  let code=null;ws.isAlive=true;ws.on("pong",()=>{ws.isAlive=true});
  ws.on("message",d=>{
    let m;try{m=JSON.parse(d)}catch{return}
    if(m.t==="join"&&!code){
      const c=String(m.room||"").toLowerCase().replace(/[^a-z0-9]/g,"").slice(0,12);
      if(!c){ws.close();return}
      let r=rooms.get(c);
      if(m.mode==="create"&&r){ws.send(JSON.stringify({t:"exists"}));ws.close();return}
      if(m.mode==="join"&&!r){ws.send(JSON.stringify({t:"none"}));ws.close();return}
      if(!r){r=new Map();rooms.set(c,r)}
      if(r.size>=2){ws.send(JSON.stringify({t:"full"}));ws.close();return}
      code=c;r.set(ws,{id:++n,p:{}});snap(code);
    }else if(m.t==="p"&&code){
      const c=rooms.get(code)&&rooms.get(code).get(ws);if(!c)return;
      const p=m.p||{};
      if(p.name!==undefined)c.p.name=String(p.name).slice(0,16);
      if(p.score!==undefined)c.p.score=Math.max(0,Math.min(50000,+p.score||0));
      if(p.done!==undefined)c.p.done=Math.max(0,Math.min(200,+p.done||0));
      if(p.aq!==undefined)c.p.aq=Math.max(0,Math.min(200,+p.aq||0));
      if(p.ar!==undefined)c.p.ar=Math.max(0,Math.min(3,+p.ar||0));
      if(p.rdy!==undefined)c.p.rdy=Math.max(0,Math.min(10,+p.rdy||0));
      if(p.t!==undefined)c.p.t=+p.t||0;
      if(p.cats!==undefined)c.p.cats=String(p.cats).slice(0,300);
      snap(code);
    }
  });
  ws.on("close",()=>{
    const r=rooms.get(code);if(!r)return;
    r.delete(ws);if(r.size===0)rooms.delete(code);else snap(code);
  });
});
setInterval(()=>{for(const ws of wss.clients){if(!ws.isAlive){ws.terminate();continue}ws.isAlive=false;ws.ping()}},25000);
server.listen(process.env.PORT||3000,()=>console.log("Quiz Challenge running"));
