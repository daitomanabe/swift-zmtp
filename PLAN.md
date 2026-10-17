# ZeroMQ の Swift 版:調査と計画

2026-10-17

## 1. 結論

- Swift から ZeroMQ を使うライブラリはすでにいくつもある。ただし、どれも **C の libzmq を包むもの(バインディング)** だった。
  ZeroMQ の通信の取り決め(ZMTP)を Swift だけで話す実装は、GitHub と Swift Package Index を探した範囲では **見つからなかった**。
- ほかの言語には、libzmq を使わない実装がある(Java の JeroMQ、C# の NetMQ、Rust の zmq.rs・omq.rs、Erlang の chumak、
  Go の go-zeromq/zmq4)。Swift にはまだ無い。
- 提案:**ZMTP 3.1 を Swift だけで実装し、libzmq や pyzmq と通信できるライブラリを作る**。
  C のライブラリ(libzmq・libsodium)、Homebrew、XCFramework をどれも要らなくし、Swift Concurrency(async/await・Sendable)で使える形にする。
- 「今すぐ Swift から使えればよい」だけなら、既存の szq で足りる(2.1)。新しく作る価値は次の 3 つにある:
  C を使わない(SwiftPM だけで入る)、iOS にもそのまま入る、async/await で書ける。

## 2. 調査

### 2.1 既存の Swift 版(どれも libzmq を包む)

| リポジトリ | 最終 push | 中身 | 状態 |
|---|---|---|---|
| [a4z/szq](https://github.com/a4z/szq) | 2026-09-02 | Swift 6。libzmq の入手は Apple では 4.3.5 の XCFramework([a4z/libzmq-xcf](https://github.com/a4z/libzmq-xcf))、Linux では pkg-config、Windows では vcpkg。ソケットの型は 12 種。API は同期で非ブロッキング(`zmq_poll` で待つ)。async/await は無い。`Socket` は class で、Sendable ではない。MPL-2.0、★3 | 今も更新されているのはこれだけ |
| [azawawi/SwiftyZeroMQ](https://github.com/azawawi/SwiftyZeroMQ) | 2019-04 | libzmq を同梱。iOS/macOS/tvOS/watchOS。★66 | 止まっている |
| [goloveychuk/ZeroMQ](https://github.com/goloveychuk/ZeroMQ)(Zewo) | 2023-03 | Swift 3。システムの libzmq。★93 | 止まっている |
| [EVOlution-App/ZeroMQ](https://github.com/EVOlution-App/ZeroMQ) | 2020-05 | Zewo の派生。Swift Package Index の Linux ビルドが失敗(2025-06) | 止まっている |
| [fanquake/swiftZMQ](https://github.com/fanquake/swiftZMQ) | 2019-07 | Swift 5、ZMQ 4.x | 止まっている |
| [azawawi/swift-zmq](https://github.com/azawawi/swift-zmq) | 2016-10 | 実験的 | 止まっている |

GitHub で言語を Swift に絞って `zmtp` を探すと 0 件だった。

### 2.2 C の libzmq を Swift で使うときの手間(ここを解く)

- **ビルドと配布**:libzmq の中身は C++。macOS は Homebrew、iOS は自分でビルドするか XCFramework、CURVE を使うなら libsodium も要る。
  SwiftPM だけでは済まない。
- **ポインタと後始末**:`zmq_msg_t`、`UnsafeMutableRawPointer`、`zmq_close` の順番など、手で管理する。
- **スレッドの決まり**:libzmq のソケットは、複数のスレッドから同時に触ってはいけない。Swift の Task はスレッドを渡り歩くので、
  actor や専用スレッドに閉じ込める工夫が要る。Swift 6 の Sendable の検査とかみ合わない。
- **待ち方**:`zmq_poll` か `ZMQ_FD`(エッジトリガで扱いにくい)を、DispatchSource や async につなぐ必要がある。

### 2.3 ほかの言語の、libzmq を使わない実装(設計の手本)

| 実装 | 言語 | 最終 push | 手本にする点 |
|---|---|---|---|
| [zeromq/zmq.rs](https://github.com/zeromq/zmq.rs) | Rust | 2026-09 | 範囲の切り方。TCP・IPC と REQ/REP/DEALER/ROUTER/PUB/SUB/XPUB/XSUB/PUSH/PULL だけで、全部はやらない |
| [paddor/omq.rs](https://github.com/paddor/omq.rs) | Rust | 2026-10 | **ネットワークを持たない ZMTP の核(sans-I/O)と、I/O の層を分ける構成**。NULL/PLAIN/CURVE。libzmq と並べたベンチ |
| [zeromq/jeromq](https://github.com/zeromq/jeromq) | Java | 2026-10 | libzmq をほぼそのまま移したもの。振る舞いの細部の参照 |
| [zeromq/netmq](https://github.com/zeromq/netmq) | C# | 2026-10 | 同上。Unity で使われている |
| [go-zeromq/zmq4](https://github.com/go-zeromq/zmq4) | Go | 2024-06 | 小さくまとまった実装 |

### 2.4 libzmq の今と仕様

- 最新の版は 4.3.5(2023-10-09)。そのあとは master の更新だけ(2026-07 まで)。通信の取り決めは何年も変わっていないので、
  作り直す相手として動かない。
- 仕様([rfc.zeromq.org](https://rfc.zeromq.org/)):23/ZMTP(3.0、stable)、37/ZMTP(3.1、draft)、24/ZMTP-PLAIN、25/ZMTP-CURVE、
  26/CurveZMQ、27/ZAP、28/REQREP、29/PUBSUB、30/PIPELINE、31/EXPAIR、32/Z85。
- libzmq 4.3.5 のソースで確かめたこと(`src/zmtp_engine.cpp`・`src/v3_1_encoder.cpp`・`src/xsub.cpp`):
  - 相手の挨拶(greeting)に書かれた版で処理を分ける。3.0 と 3.1 を区別する。1.0・2.0 の相手にも対応している。
  - 購読は、3.1 の相手には SUBSCRIBE / CANCEL のコマンドで送り、3.0 の相手には先頭に 1 バイト(0x01 / 0x00)を付けた
    メッセージで送る。受けるときはどちらの形も受ける。
  - ハートビートは PING(TTL は 0.1 秒単位、最大 16 バイトの文脈が付く)と PONG(文脈をそのまま返す)。
- CURVE に要る暗号:X25519 は CryptoKit(Linux では swift-crypto)にある。XSalsa20-Poly1305(NaCl の crypto_box)は無いので、
  Swift で書いて NaCl のテストベクタで確かめる。
- この Mac の状態:Homebrew の zeromq 4.3.5 と libsodium 1.0.22 が入っている。pyzmq は入っていない(uv で一時的に使える)。
  Swift 6.3.2。Xcode は無く、Command Line Tools だけ。Command Line Tools の Swift Testing は、SwiftPM が場所を渡さないので
  `swift test` のままでは動かない → `Scripts/test.sh` が場所を足して走らせる(2026-10-17)。

### 2.5 libzmq 同士のバイト列を記録して分かったこと(段階 0、2026-10-17)

`Tools/interop/record_fixtures.py` で pyzmq 27.2.0(libzmq 4.3.5)同士のあいだに中継を挟んで記録した。
固定データは `Tests/ZMTPTests/Fixtures/` の 9 件。段階 1 で合わせる必要がある細部:

- **挨拶を 3 回に分けて送る**:署名の 10 バイト → 相手の署名が届いたら版の上位 1 バイト → 相手の上位の版が届いたら残りの 53 バイト。
  こちらは 64 バイトを一度に送ってよい(偽の 3.0 の相手で確かめた)。受けるときは途中までの挨拶を扱えること
- **署名の 8 バイトの詰め物は 0 とは限らない**:ROUTING_ID の長さ + 1 を 64 ビットのビッグエンディアンで入れてくる(ZMTP 1.0 との互換のため)。
  ROUTING_ID が `dealer-1`(8 バイト)なら `…09`。読むときは無視する
- **as-server の 1 バイトはいつも 0**:PLAIN・CURVE のサーバーでも 0。こちらの判定に使わない
- **READY の Identity**:REQ・DEALER・ROUTER だけが送る(値が空でも送る)。PUSH・PULL・PUB・SUB・REP・PAIR は Socket-Type だけ
- **購読**:3.1 の相手には SUBSCRIBE / CANCEL のコマンド、3.0 の相手には先頭 0x01 / 0x00 のメッセージ。PUB の側で絞るので、
  購読に合わないメッセージは線の上に出ない
- **ハートビート**:PING の本体は TTL の 2 バイト(HEARTBEAT_TTL 500 ms → `0005`)だけで、文脈は空。PONG は空の文脈を返す
- **PLAIN**:HELLO の本体は 長さ 1 バイト + ユーザー名、長さ 1 バイト + パスワード。WELCOME は空。クライアントの INITIATE と
  サーバーの READY にメタデータが入る
- **CURVE の MESSAGE は、コマンドの印(0x04)を立てない普通のフレームで届く**。本体が `\x07MESSAGE` で始まる。
  段階 3 で、コマンドとしてではなくこの形で読み書きする

## 3. 何を作るか

### 3.1 目標

- SwiftPM だけで入る。C のライブラリ(libzmq・libsodium)、Homebrew、XCFramework を要らない。依存は SwiftPM だけで揃うもの
  (SwiftNIO の中の小さな C の部品は SwiftPM が自動でビルドする)
- libzmq 4.3.x と pyzmq の両方と通信できる(TCP・IPC、NULL・PLAIN・CURVE)
- Swift 6 の言語モードで警告が出ない。ソケットは Sendable で、どの Task からでも使える
- async/await と AsyncSequence で書ける
- macOS と Linux。あとで iOS

### 3.2 作らないもの

- libzmq の C API との互換層(`zmq_socket` などを提供すること)
- ZMTP 1.0 / 2.0(libzmq 3.x より前の相手)
- GSSAPI、ドラフトのソケット(CLIENT/SERVER、RADIO/DISH、SCATTER/GATHER)、STREAM、ZWS(WebSocket)、
  PGM/EPGM/NORM/TIPC/VMCI(要るとわかったら「あとで」に回す)
- オーディオスレッドなど、実時間のスレッドから直接呼ぶこと(段階 4 のあとで別に考える)

### 3.3 使い方の形(案)

```swift
import ZMQ

let context = ZMQContext()

// PUSH / PULL
let pull = try await context.socket(.pull)
let endpoint = try await pull.bind("tcp://127.0.0.1:*")   // 空いているポートを選ぶ
let push = try await context.socket(.push)
try await push.connect(endpoint)
try await push.send("hello")
let message = try await pull.receive()                    // Message(複数のフレーム)

// SUB は AsyncSequence で受ける
let sub = try await context.socket(.sub)
try await sub.subscribe("depth/")
for try await message in sub.messages { /* … */ }

// REQ は「送って受ける」を 1 回の呼び出しにする
let reply = try await req.request(Message("ping"))
```

- ソケットの型ごとに、使える操作を型で分ける(SUB にだけ `subscribe`、ROUTER は宛先の ID 付き、REQ は `request`)。
  libzmq では実行時のエラー(EFSM など)になる誤りを、コンパイルの時点で止める
- `Message` は値型(複数のフレーム)。フレームは中身を写さずに持てる型にする
- 待ち時間の上限(SNDTIMEO / RCVTIMEO にあたるもの)は、Task のキャンセルと `Duration` で表す
- 接続の出来事(つながった・切れた・再接続した)は AsyncStream で流す(`zmq_socket_monitor` にあたるもの)

### 3.4 構成

```
Package.swift
Sources/
  ZMTP/          ネットワークを持たない核(sans-I/O)。挨拶と握手の状態機械、フレームの符号化と復号、
                 コマンド(READY / ERROR / SUBSCRIBE / CANCEL / PING / PONG)、メタデータ、NULL / PLAIN / CURVE の握手。
                 バイト列を入れると、送るバイト列と出来事が出てくるだけ。ネットワークなしで単体テストできる
  ZMQ/           公開する API。ソケットの型ごとの振る舞い(負荷分散、公平な受信、購読の木、ROUTER の ID の表、
                 REQ/REP の封筒)、HWM の待ち行列、再接続、linger。TCP・IPC・inproc の I/O もここ(内部)
  ZMQCurve/      CURVE の暗号(XSalsa20・HSalsa20・Poly1305。X25519 は swift-crypto)と Z85
Tests/
  ZMTPTests/     バイト列の単体テスト。libzmq 同士の実際の挨拶と握手を記録した固定データを使う
  ZMQTests/      Swift 同士の振る舞いのテスト(inproc と 127.0.0.1)
  InteropTests/  pyzmq(と libzmq)を相手にした通信のテスト。環境変数 ZMQ_INTEROP=1 のときだけ走る
Tools/interop/   相手役の Python スクリプト(`uv run --with pyzmq` で動かす)
Benchmarks/      スループットと往復の遅延(libzmq の local_thr / local_lat と同じ測り方)
```

### 3.5 I/O の土台

| 案 | 良い点 | 弱い点 |
|---|---|---|
| **SwiftNIO(推奨)** | macOS と Linux で同じコード。iOS は NIOTransportServices(Network.framework の上)で動く。成熟している。ByteBuffer、背圧の仕組み、NIOAsyncChannel で async につなげる | 依存が増える(swift-nio・swift-atomics・swift-collections) |
| Network.framework | 依存が無い。Apple が勧める道 | Apple だけで、Linux では動かない。NWConnection の送信の待ちが際限なく伸びる落とし穴(実際のアプリでクラッシュした例がある)を自分で防ぐ必要がある |
| POSIX のソケットと kqueue / epoll を自作 | 依存が無い。いちばん細かく制御できる | 書く量がいちばん多い。OS ごとの違いを自分で持つ |

核を sans-I/O にしておくので、I/O の土台はあとから足せる(たとえば最初は SwiftNIO、iOS 向けに Network.framework 版をあとで)。

## 4. 段階

各段階の終わりに「何ができるようになったか」を、pyzmq を相手にした通信で確かめる。大きさは 小 / 中 / 大 のおおよそ。

### 段階 0:下準備(小)— 済み(2026-10-17)

- SwiftPM のパッケージの骨組み。Swift 6 の言語モード
- 相手役:`uv run --with pyzmq` で動く Python のスクリプト(pyzmq は libzmq を同梱している)
- libzmq 同士の挨拶と握手のバイト列を記録し、テストの固定データにする(NULL / PLAIN / CURVE、3.0 と 3.1)
- できあがり:`swift build` が通る。固定データが Tests に入っている
- 結果:パッケージ `swift-zmtp`(ターゲット `ZMTP`・`ZMQ`、SwiftNIO 2.104)、`Scripts/test.sh`、`Tools/interop/peer.py`
  (pyzmq 27.2.0 に固定)、`Tools/interop/record_fixtures.py`、固定データ 9 件。分かったことは 2.5

### 段階 1:最小で使える版(大)— 済み(2026-10-17)

- ZMTP 3.1 の核:挨拶(64 バイト)、版の取り決め(3.0 の相手にも合わせる)、NULL、READY のメタデータ(Socket-Type・Identity)、
  ソケットの型の組み合わせの検査、短いフレームと長いフレーム、MORE、ERROR
- TCP:connect と bind(`*` で空いているポート)、bind より先に connect してもよい、切れたら再接続(間隔を少しずつ伸ばす)
- ソケットの型:PUSH/PULL、PUB/SUB(前方一致の購読、PUB の側で絞る)、REQ/REP、DEALER/ROUTER、PAIR
- HWM(送りと受け)。HWM に達したときの型ごとの振る舞い(PUB と ROUTER は捨てる、PUSH・DEALER・REQ は待つ)。linger
- できあがり:上の型すべてで、Swift と pyzmq のあいだを両方向(Swift が bind する場合・Swift が connect する場合)にメッセージが届く。
  ベンチを 1 回測り、libzmq との差を記録する
- 結果:設計は `docs/DESIGN.md`。実装は Sonnet 5.5 のサブエージェント、確認とコミットは Opus 5.5
  - `Sources/ZMTP`(I/O を持たない核)、`Sources/ZMQ`(EventLoop 1 つにソケット 1 つを置くエンジンと 9 つの型)
  - テスト:ZMTPTests 12 本(libzmq のバイト列と一致)、ZMQTests 17 本(Swift 同士)、InteropTests 11 本・20 ケース
    (pyzmq 27.2.0 / libzmq 4.3.5 と両方向、`ZMQ_INTEROP=1` のときだけ)。libzmq との食い違いは見つからなかった
  - 再接続の間隔の既定は libzmq と同じく一定(100 ms)。伸ばすときは `reconnectIntervalMax` を決める
- ベンチ(`Benchmarks/`、1 回ずつ。Apple M5 Max、18 コア。ほかの処理が動いていたので目安。同じ設定の再計測で 15 % ほど揺れた):

  | 試験 | Swift | libzmq 4.3.5 |
  |---|---|---|
  | スループット 16 B × 200,000 | 86,557 通/秒(1.4 MB/s) | 6,711,338 通/秒(107 MB/s) |
  | スループット 1 KB × 20,000 | 84,358 通/秒(86 MB/s) | 863,154 通/秒(884 MB/s) |
  | REQ/REP の往復 16 B × 10,000 | 平均 44 µs・p50 41 µs・p99 99 µs | 平均 61 µs・p50 58 µs・p99 141 µs |

  - 往復の遅延は Swift のほうが短い。スループットは小さいメッセージで libzmq の約 1/78、1 KB で約 1/10
  - 原因の見当:`send` と `receive` のたびに EventLoop へ移る(取り消しの仕掛け、continuation、Waiter の割り当て)。
    1 通ずつ待って送ると EventLoop の 1 回の処理に 1 通しか入らず、まとめ書きが効かない(システム CPU が多かった)
  - 改善の方針(段階 4、案):送りと受けに EventLoop へ移らない近道を作る。ロックで守った送りの列に積み、空から 1 通目のときだけ
    EventLoop を起こして、たまった分をまとめて書く。受けも同じ。libzmq がアプリのスレッドと I/O スレッドを pipe でつなぐのと同じ考え方
  - libzmq 側の注意:C の計測で、送ってすぐ `zmq_close` すると(linger が無限でも)受けが途中で止まった。計測では受けが終わるまで閉じない

### 段階 2:実用の機能(中)— 済み(2026-10-17)

- IPC(Unix ドメインソケット)、inproc(同じプロセスの中。ZMTP を通さない)
- ハートビート(PING / PONG、HEARTBEAT_IVL / TIMEOUT / TTL)
- XPUB/XSUB(購読の知らせ)と proxy(XSUB と XPUB の中継)
- 複数のソケットを同時に待つ(`zmq_poll` にあたるもの)→ API は足さず、`withThrowingTaskGroup` の例を README に書いた
- よく使うオプション:ROUTER_MANDATORY、CONFLATE、IMMEDIATE、ROUTING_ID、MAXMSGSIZE、TCP_KEEPALIVE
- 接続の出来事の AsyncStream
- できあがり:IPC と inproc で段階 1 と同じ確認が通る。pyzmq の相手を止めると、ハートビートで切れたことに気づき、
  相手を戻すと再接続する
- 結果:設計は `docs/DESIGN.md` の 5 章(決めたことは 5.10)。実装は Sonnet 5.5 のサブエージェント、確認とコミットは Opus 5.5
  - 接続の抽象化(`PipeConnection`)、IPC、inproc(同じ `ZMQContext` の中、受け手に空きができるまで「済んだ」を返さない背圧)、
    ハートビートと握手の時間切れ、接続の出来事(`events()`)、`XPubSocket`・`XSubSocket`・`ZMQProxy`、
    `routerMandatory`・`conflate`・`immediate`・`tcpKeepAlive`
  - テスト:ZMTPTests 12 本、ZMQTests 42 本、InteropTests 18 本(段階 1 の 11 本と段階 2 の 7 本、pyzmq 27.2.0 / libzmq 4.3.5)
  - libzmq との食い違いを 1 つ見つけて直した:libzmq の SUB は購読のたびに SUBSCRIBE を送り、CANCEL は最後の取り消しでだけ送る。
    libzmq の PUB は相手ごとの購読を集合で持つ。こちらの PUB は数えていたので、同じ前方一致を 2 回購読した libzmq の SUB が抜けたあとも送り続けた。
    集合に直し、本物の libzmq の SUB を相手にした試験で確かめた
  - 相手のプロセスを `SIGSTOP` で止めると約 0.4 秒で `disconnected`、`SIGCONT` で戻すと再接続して次のメッセージが届いた
  - 揺れた試験が 1 本あった(libzmq の相手からのハートビートで接続が保たれる試験)。原因は試験の側で、相手が送ってすぐ閉じたのを切断と数えていた。
    相手が送ったあとも待つようにして直した。ライブラリの側の時間切れで切れた記録は無かった

### 段階 3:認証と暗号(中〜大)— 済み(2026-10-18)

- PLAIN(24/ZMTP-PLAIN)
- CURVE(25/ZMTP-CURVE・26/CurveZMQ):XSalsa20・HSalsa20・Poly1305 を Swift で書き、NaCl のテストベクタで確かめる。X25519 は swift-crypto
- Z85(鍵の文字列化)と鍵の生成
- 認証の判断(ZAP にあたるもの):許す公開鍵やユーザーを、Swift のクロージャで決める
- できあがり:pyzmq の CURVE のサーバー・クライアントと両方向で通信できる。違う鍵は拒まれる
- 外のネットワークで使う前に、暗号の部分だけを別に見直す
- 結果:設計は `docs/DESIGN.md` の 7 章。実装は Sonnet 5.5 のサブエージェント(暗号の部品と PLAIN は別の作業ツリーで並べて)、確認・暗号の見直し・コミットは Opus 5.5
  - `ZMTPCrypto`(Swift だけの Salsa20・HSalsa20・XSalsa20・Poly1305・secretbox・box と Z85。X25519 は CryptoKit、Linux は swift-crypto)。
    libsodium(PyNaCl 1.6.2)で作ったベクタと RFC 8439 の例で確かめ、Poly1305 は別に Python の多倍長計算と 11,471 通り照らし合わせた
  - PLAIN(`Security.plainClient` / `plainServer`)と CURVE(`Security.curveClient` / `curveServer`、`CurveKeyPair`・Z85)。
    認証は async のクロージャで決め、断ると ERROR "400"
  - テスト:ZMTPTests・ZMQTests・ZMTPCryptoTests 91 本、InteropTests 30 本(段階 3 の 12 本:PLAIN と CURVE を両方向で、受け入れと拒否、
    CURVE の上の購読とハートビート)
  - libzmq との食い違いを 1 つ見つけた:libzmq 4.3.5 の PLAIN の ERROR の名前は C で `"\x05ERROR"` と書かれていて、`\x05E` が 1 つの 16 進数に
    読まれるので、線の上では `5E 'RROR'` になる。受けるときはこの形も ERROR として読む。送るのは仕様の形のまま(libzmq の PLAIN クライアントは
    認証の失敗ではなく形の誤りとして扱うが、どちらでも接続は閉じる)
  - 暗号の見直し(設計の側):並び・鍵の組み合わせ・nonce の数え方・cookie と vouch の照合・一定時間の比較は仕様どおり。残っていること:
    鍵をメモリから消していない(Swift で確実に消す方法が無い)、Z85 で秘密鍵を読むときに表を引く(設定のとき 1 回)、Linux の分かれ道を
    まだビルドしていない、**外部の専門家による監査は受けていない**

### 段階 4:速さと仕上げ(中)

- **送りと受けの近道 — 段階 3 の前に済ませた(2026-10-17)**。設計は `docs/DESIGN.md` の 6 章
  - `send` と `receive` は、ロックで守った列に積む・列から取るだけで、EventLoop へ移らない。待つべきとき(相手がいない・満ちている)と
    REQ・REP は、これまでの道に落ちる
  - 同じ負荷のもとで Swift と libzmq を並べて 1 回ずつ(load average 約 17 の忙しい状態):

    | 試験 | Swift | libzmq 4.3.5 |
    |---|---|---|
    | スループット 16 B × 200,000 | 1,920,871 通/秒 | 3,996,892 通/秒 |
    | スループット 1 KB × 20,000 | 1,227,695 通/秒(1,257 MB/s) | 462,168 通/秒(473 MB/s) |
    | REQ/REP の往復 16 B | p50 148 µs・p99 514 µs | p50 168 µs・p99 396 µs |

  - 小さいメッセージは libzmq の約 1/78 から約 1/2 へ。近道の前の同じ負荷での値は 37,363 通/秒(約 50 倍)。往復の遅延は変わらない
  - テスト:ZMTPTests 12 本、ZMQTests 48 本、InteropTests 18 本が通った
- まとめ書き(複数のフレームを 1 回の書き込みに)、フレームの写しと割り当てを減らす
- ベンチ:小さいメッセージのスループットと、REQ/REP の往復の遅延を、libzmq と並べる
- iOS(NIOTransportServices)
- ドキュメント(DocC)と、zguide の例をいくつか移したもの
- 版 0.1.0

### あとでやるかもしれないもの

- ドラフトのソケット(CLIENT/SERVER、RADIO/DISH の UDP)、ZWS、STREAM
- Network.framework の I/O
- 実時間のスレッドから使える、割り当てをしない送り口

## 5. 確かめ方(minimal-testing の決まりに合わせる)

- **段階 0(変更のたび。通ったら止める)**:`swift build`。変えた所のテストだけ `swift test --filter <テスト>`。
  符号化と復号は固定データの入力 1〜3 個。通信は、変えたソケットの型 1 つで、pyzmq の相手と 1〜3 通
- **段階 1(段階 0 が NG のときだけ)**:NG の周りだけ広げる(同じソケットの型の両方向、TCP と IPC)
- **段階 2(版を出す直前に 1 回)**:全部のテストと、ソケットの型 × 方向 × TCP/IPC の通信を 1 回
- ポートは 127.0.0.1 の空いているポート(`tcp://127.0.0.1:*`)を使い、決まった番号を使わない(Port Safety の決まり)
- 通信のテストは `ZMQ_INTEROP=1` のときだけ走らせ、ふだんの `swift test` は数秒で終わるようにする

## 6. 危ないところと対策

| 危ないところ | 対策 |
|---|---|
| 仕様に書かれていない libzmq の振る舞い(REQ の再接続、ROUTER の知らない宛先、bind より先の connect、HWM の数え方など)がずれる | 振る舞いを 1 つずつ pyzmq を相手に確かめる。迷ったら libzmq のソースと `tests/` を読む(コードは写さない) |
| Swift の参照カウントや async の切り替えのせいで遅い | 段階 1 の終わりにベンチを 1 回測る。まとめ書き・まとめ読みを最初から核に入れておく |
| CURVE の暗号の誤り | テストベクタ、libzmq との通信、暗号の部分だけの見直し。外のネットワークで使う前に済ませる |
| Swift Concurrency と SwiftNIO のつなぎが込み入る | NIOAsyncChannel を使う。ソケットの状態は 1 か所(1 つの actor か EventLoop)に閉じ込める |
| 作る量に見合うか | 段階 1 で使えるところまで行き、ほかのアプリで実際に使ってから、段階 2 から先を決める |

## 7. 決めてほしいこと

1〜5 は 2026-10-17 に決まった(どれも推奨案)。

1. **方式**:Swift だけで作る — 決定
2. **I/O の土台**:SwiftNIO — 決定
3. **対応する OS の下限**:macOS 15・iOS 18・Linux(Swift 6)— 決定
4. **ライセンスと公開**:オープンソース。MIT。GitHub の公開リポジトリ `daitomanabe/swift-zmtp` — 決定。
   libzmq のコードを写さず仕様から書いているので、libzmq の MPL-2.0 には縛られない。README は英語(`README.md`)と日本語(`README.ja.md`)
5. **名前**:パッケージ `swift-zmtp`、モジュール `ZMQ` — 決定
6. **最初に使う場面**(未定):どのアプリとどのアプリをつなぐか(たとえば Mac の Swift アプリと、Linux の Python の推論サーバー)。
   段階 1 で先に作るソケットの型の順番をこれで決める

## 8. 進め方

- 設計(核の状態機械、API、振る舞いの確かめ方、レビュー)は Opus 5.5。実装は Sonnet 5.5 のサブエージェントに任せ、
  設計の側が確かめてからコミットする
- まとまりごとに、その場でコミットする

## 出典

- 既存の Swift 版:[a4z/szq](https://github.com/a4z/szq)、[a4z/libzmq-xcf](https://github.com/a4z/libzmq-xcf)、
  [azawawi/SwiftyZeroMQ](https://github.com/azawawi/SwiftyZeroMQ)、[goloveychuk/ZeroMQ](https://github.com/goloveychuk/ZeroMQ)、
  [EVOlution-App/ZeroMQ](https://github.com/EVOlution-App/ZeroMQ)、[fanquake/swiftZMQ](https://github.com/fanquake/swiftZMQ)、
  [azawawi/swift-zmq](https://github.com/azawawi/swift-zmq)、[Swift Package Index のビルド記録](https://swiftpackageindex.com/builds/98950BBE-6C8A-4E20-ABE6-55E68A90017C)
- ほかの言語の実装:[zeromq/zmq.rs](https://github.com/zeromq/zmq.rs)、[paddor/omq.rs](https://github.com/paddor/omq.rs)、
  [zeromq/jeromq](https://github.com/zeromq/jeromq)、[zeromq/netmq](https://github.com/zeromq/netmq)、
  [go-zeromq/zmq4](https://github.com/go-zeromq/zmq4)、[zeromq/chumak](https://github.com/zeromq/chumak)
- libzmq と仕様:[zeromq/libzmq](https://github.com/zeromq/libzmq)、[rfc.zeromq.org](https://rfc.zeromq.org/)
