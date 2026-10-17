# swift-zmtp(仮の名前)

ZeroMQ を Swift だけで実装する。ZMTP 3.1 を話し、libzmq・pyzmq と通信できる。
C のライブラリ(libzmq・libsodium)は使わない。調査と計画は [PLAN.md](PLAN.md)、設計は [docs/DESIGN.md](docs/DESIGN.md)。

今は段階 1 まで:TCP、NULL(認証なし)、ソケットの型 PUSH・PULL・PUB・SUB・REQ・REP・DEALER・ROUTER・PAIR。
IPC・inproc・ハートビート・XPUB/XSUB は段階 2、PLAIN・CURVE は段階 3。

```swift
import ZMQ

let pull = PullSocket()
let endpoint = try await pull.bind("tcp://127.0.0.1:*")   // 空いているポートを選ぶ
let push = PushSocket()
try await push.connect(endpoint)
try await push.send(["hello", "world"])                   // 2 フレームのメッセージ
let message = try await pull.receive()

let sub = SubSocket()
try await sub.subscribe("depth/")
for try await message in sub.messages { /* … */ }

let req = ReqSocket()
try await req.connect("tcp://192.168.0.10:5555")
let reply = try await req.request("ping")                 // REQ は送って受けるを 1 回で
```

## ビルドとテスト

```bash
swift build
```

```bash
Scripts/test.sh --filter ZMQTests
```

```bash
ZMQ_INTEROP=1 Scripts/test.sh --filter InteropTests
```

`Scripts/test.sh` は `swift test` に引数をそのまま渡す。Xcode が無く Command Line Tools だけの Mac でも Swift Testing が動くように、
フレームワークの場所を足し、`.build/clt-test` でビルドする。Xcode のある Mac や Linux では `swift test` と同じ。
`InteropTests` は pyzmq を相手にするので、`ZMQ_INTEROP=1` のときだけ走る。速さの計測は [Benchmarks/README.md](Benchmarks/README.md)。

## 相手役(libzmq)

pyzmq 27.2.0(libzmq 4.3.5 を同梱)を [uv](https://docs.astral.sh/uv/) で動かす。どちらのスクリプトも依存をファイルの先頭に書いてあるので、
`uv run` だけで動く。

- `Tools/interop/peer.py`:1 つのソケットを開き、送ったもの・受けたものを 1 行 1 件の JSON で出す。使い方は `--help`
- `Tools/interop/record_fixtures.py`:libzmq 同士のあいだに中継を挟み、両方向のバイト列を `Tests/ZMTPTests/Fixtures/` に書く

```bash
uv run Tools/interop/peer.py --type PULL --bind 'tcp://127.0.0.1:*' --recv 1
```

ポートは 127.0.0.1 の空いているポートを使い、決まった番号は使わない。
