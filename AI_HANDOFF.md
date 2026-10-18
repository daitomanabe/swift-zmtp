# AI_HANDOFF

> Contract: `portable-agent-handoff/v1`
> Captured: `2026-10-19T08:16:27+09:00`
> Scope: `swift-zmtp`(Swift だけで書いた ZeroMQ / ZMTP 3.1)を、0.1.0 を出したあとの状態で Codex と Claude Code に引き継ぐ。

## Status

- Overall: `IN_PROGRESS` — 段階 0〜3 と送り・受けの近道は済み、0.1.0 は公開済み。段階 4 の残りと、揺れた通信テストの原因調べが残っている
- Evidence freshness: `mixed` — ビルドと単体テストは 2026-10-19 に確かめた。pyzmq との通信テストは 2026-10-18 の結果(今日は流していない)
- Safe continuation: `yes` — 作業ツリーはきれい。ただし push・タグ・リリースはユーザーの指示があるときだけ(下の Safety Boundaries)

## Read First

1. `AGENTS.md` と `CLAUDE.md` — このリポジトリには**無い**(2026-10-19 に確認)。各エージェントのユーザー単位の決まりに従う。
2. `AI_HANDOFF.md` — この文書。
3. `PLAN.md` — 調査・目標・段階ごとの結果・ユーザーの決定(7 章)。日本語。**方針の正本**。
4. `docs/DESIGN.md` — 段階ごとの設計(0〜8 章)と「実装で決めたこと」(2.6・5.10・6.6・7.6)。日本語。**設計の正本**。
5. `README.md`(英語)/ `README.ja.md` / `docs/GUIDE.md`(英語の使い方の手引き)/ `CHANGELOG.md` — 公開の説明。
6. `Sources/` と `Tests/` — 実装とテスト。説明と食い違ったら、こちらが正しい。

最初に、実際の作業ツリー・ブランチ・汚れを確かめること。古いメモのブランチや場所が今も正しいと思い込まない。

## Source of Truth

| Area | Authoritative path or artifact | Evidence / conflict note |
| --- | --- | --- |
| 方針とユーザーの決定 | `PLAN.md`(7 章、段階ごとの「結果」) | `[VERIFIED]` 2026-10-19 に読んだ。「この後も Mac だけで進める」は 2026-10-18 の決定 |
| 設計 | `docs/DESIGN.md` | `[VERIFIED]` 実装の側の決定は各章の末尾の「実装で決めたこと」にある |
| ZMTP の核(I/O なし) | `Sources/ZMTP/` | `[VERIFIED]` 挨拶・フレーム・コマンド・`ZMTPConnection`・方式(NULL / PLAIN / CURVE) |
| 暗号の部品 | `Sources/ZMTPCrypto/` | `[VERIFIED]` Salsa20・HSalsa20・XSalsa20・Poly1305・secretbox・box・Z85(`package` だけ)。X25519 は CryptoKit |
| ソケットとエンジン | `Sources/ZMQ/`(`Engine/`・`Sockets/`) | `[VERIFIED]` ソケットごとに EventLoop 1 つ。型は `SocketBehavior` の派生。使い方は `Engine/SocketCore.swift` の先頭のコメント |
| libzmq の実際のバイト列 | `Tests/ZMTPTests/Fixtures/*.json` | `[VERIFIED]` `Tools/interop/record_fixtures.py` が pyzmq 27.2.0(libzmq 4.3.5)同士の通信を記録したもの。CURVE の固定データだけは記録のたびに変わる |
| 暗号のテストベクタ | `Tests/ZMTPCryptoTests/Fixtures/vectors.json` | `[VERIFIED]` `Tools/crypto/make_vectors.py`(PyNaCl 1.6.2 = libsodium)が作る。何度作っても同じ |
| libzmq の相手役 | `Tools/interop/peer.py` | `[VERIFIED]` `uv run` で動く(PEP 723 で pyzmq 27.2.0 に固定) |
| 版 | `CHANGELOG.md`、タグ `v0.1.0`(`425630d`)、GitHub のリリース | `[VERIFIED]` 2026-10-18 に公開。新しいパッケージから `from: "0.1.0"` で解決し、1 通届くことを確かめた |
| 速さの数 | `PLAN.md` 段階 1・段階 4、`README.md` の Performance | `[STALE]` 負荷の高い Mac で 1 回ずつ測った目安。比べるときは測り直す |
| ユーザー単位の決まり | 各エージェントのユーザー単位の指示ファイル | `[UNKNOWN]` この文書には写さない。下の Safety Boundaries に、このプロジェクトで確かめた決まりだけを書く |

## Current State

- `[VERIFIED]` ブランチ `main`、HEAD `1ab8c6a`、作業ツリーに変更なし。`origin/main` より **1 コミット先**(`1ab8c6a` Plan: keep working on macOS only after 0.1.0 は**未 push**)。
- `[VERIFIED]` 余分な作業ツリーは無い(`git worktree list` は本体だけ)。`.claude/worktrees/` は `.gitignore` 済みで空。
- `[VERIFIED]` 道具:Swift 6.3.2、**Command Line Tools だけで Xcode は無い**。そのため `swift test` は Swift Testing を見つけられず、`Scripts/test.sh` を使う(場所を足し、`.build/clt-test` でビルドする)。DocC の道具も無い。
- `[VERIFIED]` 依存:SwiftNIO 2.104 以上。swift-crypto(`3.0.0..<6.0.0`)は Linux のときだけ使う設定。Mac では CryptoKit。
- `[VERIFIED]` 確かめているのは **macOS だけ**(ユーザーの決定。Linux と iOS は予定に入れない)。`Package.swift` は iOS 18 と Linux の swift-crypto を宣言したまま。README・手引き・CHANGELOG には「Linux と iOS は確かめていない」と書いてある。
- `[VERIFIED]` 公開:`github.com/daitomanabe/swift-zmtp`、MIT。
- `[INFERRED]` 揺れる通信テストがある。2026-10-18 の版を切る前の確認で、InteropTests が 1 回目に 2 本落ちた(1 本は `LibzmqInteropStage3/libzmqCurveClientWithAnotherKeyIsRefusedBySwiftServer`)。単独と 2 回目では全部通った。記録を残しておらず原因は未確認で、時間に頼る試験の揺れと見ているのは推測。

## Completed and Verified

- `[VERIFIED]` ビルドと単体テスト(2026-10-19)。
  - Evidence: `swift build` → Build complete、警告なし。`Scripts/test.sh --filter "ZMTPTests|ZMQTests|ZMTPCryptoTests"` → 97 tests in 22 suites passed。
- `[VERIFIED]` pyzmq 27.2.0 / libzmq 4.3.5 との通信テスト(2026-10-18)。
  - Evidence: `ZMQ_INTEROP=1 Scripts/test.sh --filter LibzmqInterop` → 30 tests in 3 suites passed(2 回目。1 回目の揺れは Current State)。
  - 中身:段階 1 の全部のソケットの組み合わせ(TCP、Swift が bind する場合と connect する場合)、IPC、XPUB/XSUB、ハートビート(相手を `SIGSTOP` / `SIGCONT`)、PLAIN と CURVE を両方向で、受け入れと拒否、CURVE の上の購読とハートビート。
- `[VERIFIED]` 段階 0〜3 の機能:`tcp://`・`ipc://`・`inproc://`、PUSH・PULL・PUB・SUB・XPUB・XSUB・REQ・REP・DEALER・ROUTER・PAIR、`ZMQProxy.run`、方式 NULL・PLAIN・CURVE、再接続、ハートビート、握手の時間切れ、`events()`、HWM、linger、`conflate`・`immediate`・`routerMandatory`・`tcpKeepAlive`、送り・受けの近道、`receive(timeout:)`・`send(_:timeout:)`・`request(_:timeout:)`、文字列の埋め込み、`Message.string`。
- `[VERIFIED]` 見つけて直した libzmq との食い違い(`PLAN.md` と `docs/DESIGN.md` に記録あり)。
  - PUB の側は相手ごとの購読を集合で持つ。libzmq の SUB は購読のたびに SUBSCRIBE を送るため。
  - libzmq の PLAIN サーバーの ERROR は名前が `5E 'RROR'` に崩れている(C の `"\x05ERROR"`)。受けるときはこの形も読む。
- `[VERIFIED]` 例と手引き(2026-10-18)。
  - Evidence: `Examples/` が警告なしでビルドでき、各例を 1 回ずつ動かした。`docs/GUIDE.md` の Swift のコードは `Examples/` と 1 行ずつ一致することを使い捨ての照合で確かめた(照合の道具はリポジトリに無い)。
- `[VERIFIED]` 暗号の見直しは**プロジェクトの中だけ**(Opus 5.5、2026-10-18)。並び・鍵の組み合わせ・nonce・cookie と vouch の照合・一定時間の比較が仕様どおりであることを確かめた。
- `[NOT_RUN]` Linux のビルドとテスト。Reason:ユーザーの決定で Mac だけ。Required before:Linux で使うと決めたとき。
- `[NOT_RUN]` iOS(NIOTransportServices)。Reason:同上。
- `[NOT_RUN]` 外部の専門家による暗号の監査、TSAN での実行、DocC のビルド(道具が無い)。
- `[NOT_RUN]` 2026-10-19 の pyzmq との通信テスト。Reason:単体テストで足りる変更しかなかった。Required before:エンジンや方式を変えたとき。

## Pending Work

1. `[IN_PROGRESS]` 揺れた通信テストの原因を突き止める。Claude Code のセッションの提案(カード)として出してあり、まだ始まっていない。
   - Acceptance condition:落ちる確かめと原因を証拠つきで言える。試験を直すか(振る舞いと競争する決まった待ちを無くす)、ライブラリの不具合なら直して単体テストを足す。そのあと段階 3 の通信テストが 10 回続けて通る。
   - First check:`LibzmqInteropStage3` を上限つきで繰り返し、毎回の記録を残して `Expectation failed` の行を見る。
2. `[BLOCKED]` 未 push のコミット `1ab8c6a` を push する — ユーザーの指示を待つ。
3. `[UNKNOWN]` 段階 4 の残り(`PLAN.md` 段階 4):写しと割り当てを減らす(わかっているのは、CURVE の MESSAGE を開けるときの 1 回の写し)。どこが重いかは測ってから決める。DocC は Xcode を入れたとき。
4. `[UNKNOWN]` 例を書いて見えた API の改善案(`docs/DESIGN.md` 8 章の「足さないもの」)。つながるまで待つ関数、`ZMQSocket` の共通の `receive`、鍵の 16 進の文字列、`RepSocket.messages`。足すかどうかは設計の判断で、まだ決めていない。
5. `[UNKNOWN]` 予定に入れていないもの:ドラフトのソケット、STREAM、ZWS、XPUB の VERBOSE、Linux、iOS。ユーザーが求めたときだけ。

## Blockers and Decisions Needed

- `[BLOCKED]` push・タグ・GitHub のリリースは、毎回ユーザーの指示が要る。安全な代わりの道は無い(手元でコミットまでにする)。
- `[VERIFIED]` 決定済み:Mac だけで進める(2026-10-18)。Linux や iOS の作業を勧めない。
- `[UNKNOWN]` 1.0 まで API は変わってよいが、どこを固めるかはまだ決めていない。勝手に決めない。
- `[UNKNOWN]` 信頼できないネットワークで CURVE を使う前の外部の監査:受けるかどうかはユーザーの判断。それまでは README・手引き・CHANGELOG の「監査は受けていない」をそのまま残す。

## Reproduction / Verification

Working directory: `${PROJECT_ROOT}`(このリポジトリの root)

```sh
cd "${PROJECT_ROOT}"
# まず状態を見る
git status --short --branch
git worktree list
# ビルド(警告 0 のはず)
swift build
# 単体テスト(Command Line Tools だけの Mac では swift test ではなくこちら)
Scripts/test.sh --filter "ZMTPTests|ZMQTests|ZMTPCryptoTests"
# 変えた所だけを走らせるとき(例)
Scripts/test.sh --filter CurveTests
```

Expected result:`Build complete!`、`Test run with 97 tests in 22 suites passed`(テストを足せば数は増える)。

pyzmq との通信テスト(`uv` が要る。相手役は `uv run Tools/interop/peer.py` で、pyzmq 27.2.0 が入る):

```sh
cd "${PROJECT_ROOT}"
ZMQ_INTEROP=1 Scripts/test.sh --filter LibzmqInterop
pgrep -fl '[p]eer.py' || echo "no peer.py left"
```

Expected result:`30 tests in 3 suites passed`、`peer.py` が残っていない。揺れについては Current State を見ること。

例と計測(別のパッケージ。ルートのパッケージを path で使う):

```sh
cd "${PROJECT_ROOT}/Examples" && swift build
cd "${PROJECT_ROOT}/Benchmarks" && swift build -c release && .build/release/zmq-bench thr 200000 16
```

ルートにファイルが増えたあとで `cannot find type …` で失敗したら、その別パッケージの `.build`(`Examples/.build` か `Benchmarks/.build`)を消してビルドし直す。SwiftPM が、すでにあるビルドの場所では path で使うパッケージの新しいファイルを拾わなかった。

Checks not run: `[NOT_RUN]` Linux・iOS・TSAN・DocC・外部の監査 — 理由は Completed and Verified。

## Safety Boundaries

- `[VERIFIED]` **push・タグ・リリース・公開範囲の変更はユーザーの指示があるときだけ**。コミットは確認なしでよく、論理的なまとまりごとにその場でする(ユーザーの決まり。このプロジェクトでずっとそうしてきた)。
- `[VERIFIED]` **libzmq のコードを写さない**。仕様(rfc.zeromq.org の 23・24・25・26・27・28・29・30・31・32・37)と、記録した通信から書く。MIT で出しているのはこのため。libzmq のソースは振る舞いを確かめるために読むだけ。
- `[VERIFIED]` `Sources/ZMTPCrypto/` を変えたら、暗号のベクタのテストと CURVE の通信テストを通し、暗号の部分を見直してからコミットする。README・手引き・CHANGELOG の「外部の監査は受けていない」を消さない。
- `[VERIFIED]` テストのポートは `127.0.0.1` と `*`(空いているポート)だけで、決まった番号は使わない。例のパッケージは決まったポート(5555 など)を既定にしているので、動かす前に `lsof -i:<port> -P` で持ち主を確かめ、ほかのプロジェクトのプロセスを止めない。
- `[VERIFIED]` `peer.py` を `SIGSTOP` する試験がある。止めたプロセスを残さない(`PeerProcess.terminate()` は先に `SIGCONT` を送る)。走らせたあとは `pgrep -fl '[p]eer.py'` が空であること。
- `[VERIFIED]` 確かめは最小から:ビルド、変えた所のテストだけ、通信は 1〜3 通。NG のときだけ広げる。全部のテストは版を切る前に 1 回。
- `[VERIFIED]` `Tests/ZMTPTests/Fixtures/` は記録したデータ。手で直さず、変えるときは `uv run Tools/interop/record_fixtures.py` で記録し直す(CURVE の固定データは毎回変わる)。
- `[UNKNOWN]` GitHub の認証はこの Mac の `gh` にある。値をこの文書や記録に写さない。

## Next Agent

Use this sequence:

1. Read First の順に読む。とくに `PLAN.md` の 7 章と段階 4、`docs/DESIGN.md` の該当の章。
2. 作業ツリーとブランチと汚れを確かめる(`git status --short --branch`、`git worktree list`)。`1ab8c6a` がまだ未 push かを見る。
3. `swift build` と `Scripts/test.sh --filter "ZMTPTests|ZMQTests|ZMTPCryptoTests"` を走らせる。
4. ユーザーの指示があれば、Pending Work の 1 番(揺れの原因調べ)から始める。その受け入れの条件が今も当てはまるかを先に確かめる。
5. 終える前に、新しい証拠でこの文書を直す(Status・Current State・Change Log)。

会話の要約をこの文書の代わりにしない。受け入れの証拠が揃うまで、終わったと言わない。

## Change Log

| Timestamp | Agent / session label | Change | Evidence |
| --- | --- | --- | --- |
| `2026-10-19` | `Claude Code (Opus 5.5)` | 最初に書いた | `swift build`、`Scripts/test.sh --filter "ZMTPTests|ZMQTests|ZMTPCryptoTests"`(97 passed)、`git status --short --branch` |
