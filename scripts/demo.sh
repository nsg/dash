#!/usr/bin/env bash

set -u

dash_url=${DASH_URL:-http://127.0.0.1:9090}
walk=0

while true; do
  printf -v epoch_second '%(%s)T' -1
  batch=$(awk -v t="$epoch_second" -v walk="$walk" 'BEGIN {
    pi = atan2(0, -1)
    sine = sin(t * pi / 30)
    cosine = cos(t * pi / 30)
    sawtooth = (t % 60) / 30 - 1
    square = sine >= 0 ? 1 : -1
    srand(t)
    noise = rand() * 2 - 1
    noise2 = rand() * 2 - 1
    noise3 = rand() * 2 - 1
    walk += noise * 0.4
    if (walk > 5) walk = 5
    if (walk < -5) walk = -5
    frontend_requests = 120 + 22 * sine + 8 * noise
    frontend_latency = 55 + 8 * cosine + 4 * noise2
    backend_requests = 95 + 18 * cosine + 7 * noise2
    backend_latency = 80 + 14 * sine + 5 * noise3
    cpu_load = 42 + 15 * sine + 5 * noise
    mem_used = 2048 + 180 * cosine + 20 * noise2
    net_rx = 320 + 80 * sawtooth + 30 * noise3
    net_tx = 180 + 50 * cosine + 20 * noise
    printf "%.8f\n", walk
    printf "[{\"m\":\"demo.wave.sine\",\"v\":%.8f},{\"m\":\"demo.wave.cosine\",\"v\":%.8f},{\"m\":\"demo.wave.sawtooth\",\"v\":%.8f},{\"m\":\"demo.wave.square\",\"v\":%.8f},{\"m\":\"demo.rand.noise\",\"v\":%.8f},{\"m\":\"demo.rand.walk\",\"v\":%.8f},{\"m\":\"web.frontend.requests\",\"v\":%.8f},{\"m\":\"web.frontend.latency_ms\",\"v\":%.8f},{\"m\":\"web.backend.requests\",\"v\":%.8f},{\"m\":\"web.backend.latency_ms\",\"v\":%.8f},{\"m\":\"sys.cpu.load\",\"v\":%.8f},{\"m\":\"sys.mem.used_mb\",\"v\":%.8f},{\"m\":\"sys.net.rx_kbps\",\"v\":%.8f},{\"m\":\"sys.net.tx_kbps\",\"v\":%.8f}]", sine, cosine, sawtooth, square, noise, walk, frontend_requests, frontend_latency, backend_requests, backend_latency, cpu_load, mem_used, net_rx, net_tx
  }')
  walk=${batch%%$'\n'*}
  payload=${batch#*$'\n'}
  curl -sS "$dash_url/ingest" -H 'content-type: application/json' -d "$payload"
  printf '\n'
  sleep 2
done
