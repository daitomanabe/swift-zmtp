# 設計:段階 1(ZMTP の核・TCP・主なソケットの型)

2026-10-17。PLAN.md の段階 1 を作るための設計。実装は Sonnet 5.5 のサブエージェントに任せ、設計の側(Opus 5.5)が確かめてからコミットする。
ここに無い細部は実装の側で決めてよい。決めたことと、設計から外れたことは報告に書く。

## 0. 全体の決まり

- `ZMTP`(`Sources/ZMTP`):I/O を持たない核。依存は NIOCore だけ(`ByteBuffer` を使い、フレームの本体を写さずに切り出すため)
- `ZMQ`(`Sources/ZMQ`):公開する API。ソケットの型ごとの振る舞い、pipe と HWM、TCP(SwiftNIO の NIOPosix)
- Swift 6 の言語モード。**警告 0**。`@unchecked Sendable` は最小限にし、使うところに理由をコメントで書く
- コードのコメントは英語。公開する型とメソッドには短い doc コメント
- 公開 API に NIO の型を出さない(`ZMQContext(eventLoopGroup:)` だけは例外)。`ZMTP` と `ZMQ` のあいだは `package` か `public` でよい
- libzmq のコードは写さない。仕様(rfc.zeromq.org の 23・37・28・29・30・31)と、PLAN.md 2.5 の記録した事実に従う
- テストは Swift Testing(`import Testing`)。この Mac では `Scripts/test.sh` で走らせる(Command Line Tools のため)

## 1. ZMTP モジュール

### 1.1 SocketType

```swift
public enum SocketType: String, Sendable, CaseIterable {
    case pair = "PAIR", pub = "PUB", sub = "SUB", req = "REQ", rep = "REP"
    case dealer = "DEALER", router = "ROUTER", pull = "PULL", push = "PUSH"
    case xpub = "XPUB", xsub = "XSUB"
    public func isCompatible(with peer: SocketType) -> Bool
    /// READY carries an Identity property (libzmq does this for REQ, DEALER, ROUTER, even when empty).
    public var sendsIdentity: Bool
}
```

組み合わせ(23/ZMTP、libzmq と同じ):REQ–REP、REQ–ROUTER、REP–DEALER、DEALER–DEALER、DEALER–ROUTER、ROUTER–ROUTER、
PUB–SUB、PUB–XSUB、XPUB–SUB、XPUB–XSUB、PUSH–PULL、PAIR–PAIR。

### 1.2 挨拶(greeting、64 バイト)

| バイト | 中身 | 書くとき | 読むとき |
|---|---|---|---|
| 0 | 0xFF | 0xFF | 0xFF でなければ `badSignature` |
| 1–8 | 詰め物 | libzmq と同じく「routing ID の長さ + 1」を UInt64 のビッグエンディアンで | 無視する |
| 9 | 0x7F | 0x7F | bit 0 が 0 なら ZMTP 1.0 → `badSignature` |
| 10 | 版の上位 | 3 | 3 未満は `unsupportedVersion(major:)`。3 より大きければ 3.1 として扱う |
| 11 | 版の下位 | 1 | 覚えておく(0 なら 3.0 の相手) |
| 12–31 | 方式の名前 | "NULL" を NUL で埋める | こちらの方式と違えば `mechanismMismatch` |
| 32 | as-server | 0 | 無視する(libzmq はいつも 0) |
| 33–63 | 埋め草 | 0 | 無視する |

- 署名は届いたところから確かめる:10 バイトそろったら 0 と 9 を、11 バイトそろったら 10 を見る。相手が ZMTP でなければ早く切る
- libzmq は挨拶を 10 → 1 → 53 バイトに分けて送ってくる。こちらは 64 バイトを一度に送る

### 1.3 フレーム

- flags の 1 バイト:bit 0 = MORE、bit 1 = LONG(大きさが 8 バイト)、bit 2 = COMMAND。bit 3–7 が立っていたら `malformedFrame`
- 大きさ:LONG なら UInt64 のビッグエンディアン、でなければ 1 バイト。書くときは 255 以下を短い形、それより大きければ長い形にする
- COMMAND と MORE が同時に立っていたら `malformedFrame`(23/ZMTP の文法で command の flags は 0x04 か 0x06 だけ)
- `maxMessageSize`(あれば)は**フレームごと**の上限(libzmq の MAXMSGSIZE と同じ)。超えたら `messageTooLarge`
- 本体は受け取った `ByteBuffer` から `readSlice` で切り出し、写さない

### 1.4 コマンド

コマンドの本体は「名前の長さ 1 バイト + 名前 + データ」。

| 名前 | データ |
|---|---|
| READY | メタデータ(下) |
| ERROR | 理由の長さ 1 バイト + 理由(ASCII) |
| SUBSCRIBE | 購読の前方一致の文字列(残り全部) |
| CANCEL | 同上 |
| PING | TTL 2 バイト(ビッグエンディアン、0.1 秒単位)+ 文脈(0〜16 バイト) |
| PONG | 文脈(PING の文脈をそのまま) |
| ほか | `unknown(name:body:)` として通す(HELLO・WELCOME・INITIATE は段階 3) |

メタデータ:「名前の長さ 1 バイト(1〜255)+ 名前 + 値の長さ UInt32 ビッグエンディアン + 値」の繰り返し。名前は大文字小文字を区別しない。
順番を保つ。READY に入れる順番は libzmq と同じく Socket-Type、Identity。

```swift
public enum Command: Sendable, Equatable {
    case ready(Metadata)
    case error(reason: String)
    case subscribe(topic: ByteBuffer)
    case cancel(topic: ByteBuffer)
    case ping(ttl: UInt16, context: ByteBuffer)
    case pong(context: ByteBuffer)
    case unknown(name: String, body: ByteBuffer)
    public init(body: ByteBuffer) throws          // body of a COMMAND frame
    public func write(into buffer: inout ByteBuffer)  // the whole frame: flags, size, body
}

public struct Metadata: Sendable, Equatable {
    public struct Property: Sendable, Equatable { public var name: String; public var value: ByteBuffer }
    public var properties: [Property]
    public subscript(name: String) -> ByteBuffer? { get }   // case-insensitive
    public var socketType: String? { get }                  // raw value of Socket-Type
    public var identity: ByteBuffer? { get }
}
```

### 1.5 ZMTPConnection(状態機械)

1 本の接続の ZMTP を、バイト列だけで扱う。I/O・時刻・スレッドを持たない。

```swift
public struct ZMTPConnection: Sendable {
    public struct Configuration: Sendable {
        public var socketType: SocketType
        public var routingID: [UInt8]          // goes into Identity when socketType.sendsIdentity
        public var maxMessageSize: Int?
        public init(socketType: SocketType, routingID: [UInt8] = [], maxMessageSize: Int? = nil)
    }

    public enum Output: Sendable, Equatable {
        case write(ByteBuffer)                         // send to the peer, in this order
        case ready(peer: Metadata, minorVersion: UInt8) // handshake done
        case message([ByteBuffer])                     // one complete multipart message (data frames)
        case subscribe(ByteBuffer)                     // the peer subscribed (we are PUB or XPUB)
        case cancel(ByteBuffer)
        case closed(ZMTPError)                         // close the transport; nothing follows
    }

    public init(configuration: Configuration)
    public mutating func start() -> [Output]                    // [.write(greeting)]
    public mutating func receive(_ bytes: ByteBuffer) -> [Output] // keeps partial input inside
    public var isReady: Bool { get }
    public var peerMinorVersion: UInt8? { get }
    public func encodeMessage(_ frames: [ByteBuffer], into buffer: inout ByteBuffer)   // precondition: ready, frames non-empty
    public func encodeSubscribe(_ topic: ByteBuffer, into buffer: inout ByteBuffer)
    public func encodeCancel(_ topic: ByteBuffer, into buffer: inout ByteBuffer)
}

public enum ZMTPError: Error, Sendable, Equatable {
    case badSignature
    case unsupportedVersion(major: UInt8)
    case mechanismMismatch(local: String, peer: String)
    case malformedFrame(String)
    case malformedCommand(String)
    case messageTooLarge
    case missingSocketType
    case incompatibleSocketType(local: SocketType, peer: String)
    case unexpected(String)            // e.g. a data frame before READY, a second READY
    case peerError(reason: String)     // the peer sent ERROR
}
```

流れ(NULL):

1. `start()` で挨拶を書く
2. 相手の挨拶が 64 バイトそろったら確かめ(1.2)、READY を書く(libzmq と同じ順番。挨拶の交換が済んでから READY)
3. 最初のコマンドは READY でなければならない。ERROR なら `closed(.peerError)`。READY の Socket-Type が無ければ `missingSocketType`、
   組み合わせが合わなければ ERROR コマンド(理由は "Invalid socket type")を書いてから `closed(.incompatibleSocketType)`
4. 握手のあと:
   - データフレームを MORE でつなぎ、最後のフレームで `.message`
   - PING → PONG(同じ文脈)を `.write` で返す。PONG は無視。未知のコマンドは無視
   - SUBSCRIBE / CANCEL → `.subscribe` / `.cancel`
   - ERROR → `closed(.peerError)`。2 回目の READY → `closed(.unexpected)`
   - こちらが PUB か XPUB のとき:**フレーム 1 つだけのメッセージで、先頭が 0x01 / 0x00** なら `.subscribe` / `.cancel`(残りが前方一致)。
     3.0 の相手はこの形で購読を送ってくる(PLAN.md 2.5)
5. `closed` を出したあとの `receive` は何も出さない
6. `encodeSubscribe` / `encodeCancel`:相手の下位の版が 1 以上なら SUBSCRIBE / CANCEL コマンド、0 なら先頭 0x01 / 0x00 のデータフレーム
7. 1 回の `receive` で出る `Output` は、起きた順に並べる(例:`[.write(READY), .ready, .message]`)

段階 3 への備え:方式は内部の enum にし、`encodeMessage` と受けたデータフレームが方式を通る形にしておく(CURVE が暗号化を差し込めるように)。
NULL では何もしない。実装済み(`Sources/ZMTP/Mechanism.swift`):握手のあとのフレームはコマンドも含めてすべて `Mechanism.encode` / `decode` を通る。
CURVE は送るたびに nonce を数えるので、段階 3 で `encodeMessage`・`encodeSubscribe`・`encodeCancel` を `mutating` にする。

### 1.6 ZMTP のテスト(`Tests/ZMTPTests`、固定データを使う)

1. **こちらのバイト列が libzmq と同じ**:クライアントと同じ設定で `start()` し、`serverToClient` を流したときに書いたバイト列が、
   `clientToServer` の先頭(挨拶 + READY)と一致する。null-push-pull(PUSH)、null-req-rep(REQ)、null-dealer-router(DEALER、routing ID `dealer-1`)
2. null-push-pull の `serverToClient` を PUSH の接続に流す → `.ready`、Socket-Type は PULL
3. null-push-pull の `clientToServer` を PULL の接続に流す → `["hello"]`、`["part1", "part2"]`、`["x" × 300]`。1 バイトずつ流しても同じ
4. null-pub-sub の `clientToServer` を PUB の接続に流す → `.subscribe("w")`、`.cancel("w")`
5. zmtp30-sub の `clientToServer` を PUB の接続に流す → `.subscribe("w")`、`.cancel("w")`(0x01 / 0x00 の形)
6. `encodeSubscribe("w")`:zmtp30-sub の `serverToClient`(下位 0)を流した SUB では `00 02 01 77`、null-pub-sub の `serverToClient`(下位 1)を
   流した SUB では `04 0b 09 'SUBSCRIBE' 77`
7. null-heartbeat の `clientToServer` を PULL の接続に流す → `.message(["hello"])` と、PONG のフレーム `04 05 04 'PONG'` の `.write`
8. 誤り 1 つずつ:先頭が 0x00 → `badSignature`。方式が PLAIN → `mechanismMismatch`。PUSH の設定に PUSH の READY → ERROR を書いて `incompatibleSocketType`

## 2. ZMQ モジュール

### 2.1 公開する型

```swift
public struct Frame: Sendable, Hashable, ExpressibleByStringLiteral {
    public init()                            // empty frame
    public init(_ bytes: some Sequence<UInt8>)
    public init(_ string: String)            // UTF-8
    public init(_ data: Data)
    public var count: Int { get }
    public var isEmpty: Bool { get }
    public var bytes: [UInt8] { get }
    public var data: Data { get }
    public var string: String? { get }       // nil when not valid UTF-8
    public func withUnsafeBytes<R>(_ body: (UnsafeRawBufferPointer) throws -> R) rethrows -> R
    package init(buffer: ByteBuffer)         // no copy
    package var buffer: ByteBuffer { get }
}

public struct Message: Sendable, Hashable, ExpressibleByStringLiteral, ExpressibleByArrayLiteral {  // element: Frame
    public var frames: [Frame]
    public init(_ frames: [Frame])
    public init(_ frame: Frame)
    public init(_ string: String)
    public init(_ data: Data)
}

public struct RoutingID: Sendable, Hashable { public var frame: Frame; public init(_ frame: Frame) }
public struct RoutedMessage: Sendable, Hashable { public var routingID: RoutingID; public var message: Message }

public struct SocketOptions: Sendable {
    public var sendHighWaterMark = 1000
    public var receiveHighWaterMark = 1000
    public var linger: Duration? = .seconds(1)          // nil: wait until everything is sent
    public var routingID: RoutingID? = nil
    public var reconnectInterval: Duration = .milliseconds(100)
    public var reconnectIntervalMax: Duration? = nil     // set: double after each failure, up to this
    public var maxMessageSize: Int? = nil                // per frame
    public init() {}
}

public enum ZMQError: Error, Sendable, Equatable {
    case invalidEndpoint(String)
    case unsupportedTransport(String)      // ipc:// and inproc:// until stage 2
    case bindFailed(endpoint: String, reason: String)
    case socketClosed
    case invalidState(String)              // e.g. REP reply without a request
}

public final class ZMQContext: Sendable {
    public static let `default`: ZMQContext
    public init()                                             // MultiThreadedEventLoopGroup.singleton
    public init(eventLoopGroup: any EventLoopGroup)
}

public protocol ZMQSocket: Sendable {
    var options: SocketOptions { get }
    @discardableResult func bind(_ endpoint: String) async throws -> String   // the endpoint actually bound
    func connect(_ endpoint: String) async throws
    func close() async
}

/// Calls receive() until the socket closes; then ends (no error).
public struct MessageSequence<Element: Sendable>: AsyncSequence, Sendable { … }
```

ソケット(すべて `public final class`、`init(context: ZMQContext = .default, options: SocketOptions = SocketOptions())`):

| 型 | 操作 |
|---|---|
| `PushSocket` | `send(_ message: Message) async throws` |
| `PullSocket` | `receive() async throws -> Message`、`messages: MessageSequence<Message>` |
| `PairSocket` | `send`、`receive`、`messages` |
| `PubSocket` | `send`(相手を待たない) |
| `SubSocket` | `subscribe(_ prefix: Frame = Frame()) async throws`、`unsubscribe(_ prefix: Frame) async throws`、`receive`、`messages` |
| `ReqSocket` | `request(_ message: Message) async throws -> Message` |
| `RepSocket` | `receive() async throws -> Message`、`reply(_ message: Message) async throws` |
| `DealerSocket` | `send`、`receive`、`messages` |
| `RouterSocket` | `receive() async throws -> RoutedMessage`、`send(_ message: Message, to: RoutingID) async throws`、`messages: MessageSequence<RoutedMessage>` |

使い方の例は PLAN.md 3.3(名前はこの表に合わせる)。

### 2.2 エンジン(内部)

**スレッドの持ち方:ソケットごとに EventLoop を 1 つ決め、そのソケットの状態とすべての Channel をその上に置く**(libzmq の I/O スレッドと同じ考え方)。

- ソケットを作るときに `group.next()` で EventLoop を選ぶ。ServerBootstrap と ClientBootstrap はその EventLoop だけで作る(child も同じ)
- 状態はその EventLoop の上でだけ触る(`NIOLoopBoundBox` など)。ロックは使わない。Channel のハンドラーからは同じスレッドなので直接呼べる
- 公開する async メソッドは EventLoop へ移って処理し、`EventLoopPromise` で結果を待つ
- 待ち(受信を待つ、送れる先を待つ)は FIFO の列に入れる。**取り消し**:`withTaskCancellationHandler` で、取り消されたら EventLoop の上で
  その待ちを外して `CancellationError` で終える。取り消しの知らせが待ちの登録より先に EventLoop に着くことがあるので、
  見つからなかった ID は「取り消し済み」として覚え、あとから来た登録をすぐ終える

**pipe**:相手 1 つごとの送りと受けの列。

- 送り:`pending` = まだ Channel に渡していないメッセージ + 渡したが書き込みが終わっていないメッセージ。`pending < sendHighWaterMark` なら受け付けられる
- Channel への書き込みは `write` を重ね、EventLoop の 1 回の処理の終わりに 1 回だけ `flush` する(メッセージごとに flush しない)
- 受け:pipe ごとの列(上限 `receiveHighWaterMark`)。列が満ちたら Channel から読まない(`autoRead` を切り、空いたら `read()`)。TCP で相手に背圧が伝わる
- **connect の側**:`connect()` を呼んだ時点で pipe を作る(握手の前から送り先として使え、メッセージは pipe にたまる。libzmq と同じ)。
  握手が済んだら Channel につなぎ、たまったものを流す。切れたら Channel から外し、書き込み中だったものは捨て、たまっているものは残して
  `reconnectInterval` のあとにつなぎ直す(`reconnectIntervalMax` があれば失敗ごとに倍、上限まで)。つなぐ失敗も同じ間隔でやり直す
- **bind の側**:受け入れた接続ごとに、握手が済んだら pipe を作る。切れたら pipe ごと捨てる

**閉じる**(`close()`):送りの待ちと受けの待ちを `socketClosed` で終える → listener を閉じる → 各 pipe の `pending` が 0 になるか `linger` が過ぎるまで待つ
(そのあいだ connect の側はつなぎ直しを続けてよい)→ すべての Channel を閉じ、つなぎ直しの予約を消す。2 回目からは何もしない。
閉じたあとの操作は `socketClosed`。`close()` を呼ばずにソケットが解放されたら、待たずに閉じる。

**型ごとの振る舞い**:エンジンは型ごとの部分(送り先の選び方、受けたものの扱い)を差し替えられる形にする。共通の部品:

- 負荷分散(load balance):受け付けられる pipe を順番に回す。どこも受け付けられなければ待つ
- 公平な受信(fair queue):受けたメッセージのある pipe を順番に回す

### 2.3 TCP

- endpoint:`tcp://HOST:PORT`。bind の HOST は IPv4 の番地か `*`(= 0.0.0.0)、PORT は数か `*`(= 空いているポート)。
  connect の HOST は名前でもよい。`ipc://` と `inproc://` は `unsupportedTransport`(段階 2)。ほかは `invalidEndpoint`
- `bind` は実際の endpoint を返す(`tcp://127.0.0.1:*` → `tcp://127.0.0.1:54321`、`*` は `0.0.0.0`)
- Channel:TCP_NODELAY を入れる。listener は SO_REUSEADDR。`autoRead` は切る(2.2)
- 1 つのソケットで bind と connect を何度呼んでもよい

### 2.4 型ごとの振る舞い(libzmq に合わせる)

| 型 | 送り | 受け |
|---|---|---|
| PUSH | 負荷分散。送れる先が無ければ待つ | ― |
| PULL | ― | 公平な受信 |
| PAIR | 相手は 1 つだけ(2 つ目の接続は閉じる)。満ちていれば待つ | その相手から |
| PUB | 購読が前方一致する pipe すべてへ。満ちた pipe には捨てる。待たない。購読者がいなければ捨てる | ―(受けたデータは捨てる) |
| SUB | ― | 公平な受信。自分の購読でも絞る(念のため) |
| REQ | 先頭に空のフレームを足し、負荷分散で 1 つへ。その pipe からの返事だけを待つ | 返事の先頭の空フレームを外して返す。空フレームが無い返事・ほかの pipe からの返事は捨てる |
| REP | 受けたメッセージの「空フレームまで」(封筒)を覚えた pipe へ、封筒 + 返事を送る。pipe が消えていれば捨てる | 公平な受信。封筒を外して本体を返す。空フレームの無いものは捨てる |
| DEALER | 負荷分散。待つ | 公平な受信 |
| ROUTER | 先頭の routing ID の pipe へ。知らない ID・満ちた pipe には捨てる | 公平な受信。送り元の routing ID を付けて返す |

- **購読の数え方**:SUB は前方一致ごとに数を持ち、0 → 1 のときだけ SUBSCRIBE、1 → 0 のときだけ CANCEL を全 pipe へ送る。
  pipe の握手が済むたびに、持っている前方一致を 1 つずつ送る(つなぎ直しのあとも)。PUB は pipe ごとに前方一致の数を持つ(重なりも数える)
- **ROUTER の routing ID**:相手の Identity が空でなければそれ。すでに使われていれば新しい接続を閉じる(libzmq の既定)。
  空なら 5 バイト `00` + UInt32(ビッグエンディアン、乱数から始めて 1 ずつ増やす)
- **REQ の取り消し**:`request` が取り消されたら送れる状態に戻す。その pipe に「捨てる返事 1 つ」を数えておき、次にその pipe から来た返事を 1 つ捨てる
  (同じ相手は順番に返すので、古い返事を捨てて新しい返事を取れる)。返事の来ない相手は、使う側が取り消しで抜ける(libzmq と同じく待ち続ける)
- **REP**:`receive` のあと `reply` の前にもう一度 `receive` を呼んだら `invalidState`。`reply` を `receive` の前に呼んでも `invalidState`

### 2.5 ZMQ のテスト(`Tests/ZMQTests`、Swift 同士、`tcp://127.0.0.1:*`)

型ごとに 1 本(送って届く)に加えて、振る舞いを 1 本ずつ:bind より先の connect でたまったものが届く、相手が止まってまた bind されたらつなぎ直して届く、
HWM で PUSH が待つ、PUB が満ちた pipe に捨てる、REQ の取り消しのあと次の request が正しい返事を受ける、受信の取り消し。

### 2.6 実装で決めたこと(エンジン、2026-10-17)

- 型の差し替えは `SocketBehavior` の派生クラス(`Sources/ZMQ/Engine/SocketBehavior.swift`、使い方は `SocketCore.swift` の先頭)
- 取り消しの順番の問題は「取り消し済みの ID の記録」ではなく、`Waiter` が自分の状態(未登録・待ち・取り消し済み・済み)を持って解く
  (済んだあとの取り消しで記録が残り続けないように)
- PAIR の 2 回目の `connect()` は `invalidState` を投げる(libzmq は黙って無視する)。bind の側の 2 つ目の接続は閉じる
- `close()` を待っている Task が取り消されたら、linger を待たずに閉じる(`linger = nil` でも取り消しで抜けられる)
- HWM が 0 以下なら上限なし。受けの HWM は、1 回の読み込みに入っていた分だけ超えることがある
- 空の `Message`(フレーム 0 個)の送信は `invalidState`
- bind の側の pipe は、接続が切れても受け取り済みのメッセージが読まれるまでは残る(送ってすぐ閉じた相手のメッセージを失わない)

## 3. pyzmq との通信のテスト(`Tests/InteropTests`)

- `ZMQ_INTEROP=1` のときだけ走る(Swift Testing の `.enabled(if:)`)。相手は `uv run Tools/interop/peer.py …` を `Process` で起こし、
  1 行 1 件の JSON を読む。`ready` の行から endpoint を取る。上限の時間を必ず付ける
- 組み合わせ(それぞれ Swift が bind する場合と connect する場合):PUSH→PULL、PULL←PUSH、PUB→SUB、SUB←XPUB(`--wait-sub 1`)、
  REQ→REP、REP←REQ、REQ→ROUTER、DEALER→ROUTER、ROUTER←DEALER、DEALER→REP、PAIR↔PAIR

## 4. エージェントへの確かめ方の指示(minimal-testing)

- 段階 0 だけ:ビルド(警告 0)、自分が足した・変えたテストだけ `Scripts/test.sh --filter <名前>`、通信は 1〜3 通
- NG なら NG の周りだけ広げて原因を絞り、何が NG で何を広げたかを報告する
- 全部のテストを流し直さない。git は触らない(コミットは設計の側がする)
