#!/usr/bin/env bash
# Run ON THE VPS:  bash diagnose.sh   -> paste the whole output back
PORT=${1:-8080}
echo "== 1. container / process"; docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' 2>/dev/null || echo "docker not found"
echo; echo "== 2. listening on $PORT?"; (ss -ltnp 2>/dev/null || netstat -ltnp 2>/dev/null) | grep ":$PORT " || echo "NOTHING listening on $PORT"
echo; echo "== 3. local request"; curl -s -m 8 -o /dev/null -w "localhost:$PORT/health -> HTTP %{http_code}\n" http://127.0.0.1:$PORT/health || echo "local curl failed"
PUB=$(curl -s -m 8 https://api.ipify.org); echo; echo "== 4. public IP of this server: ${PUB:-unknown}"
curl -s -m 8 -o /dev/null -w "public-ip:$PORT/health (from this server) -> HTTP %{http_code}\n" http://$PUB:$PORT/health || echo "public IP request failed (firewall / security group)"
echo; echo "== 5. OS firewall"; (command -v ufw >/dev/null && sudo ufw status) ; (sudo iptables -S 2>/dev/null | grep -E "DROP|REJECT|$PORT" | head)
echo; echo "== 6. last app logs"; docker logs --tail 30 $(docker ps -q | head -1) 2>&1 | grep -E "Web server|base URL|Uvicorn|Error|error" | tail -10
echo; echo "Next: from your PC/phone open  http://$PUB:$PORT/health  -> {\"status\":\"ok\"} means everything works."
echo "If step 3 is 200 but the PC/phone can't open it: open TCP $PORT inbound in the cloud Security Group (0.0.0.0/0)."
