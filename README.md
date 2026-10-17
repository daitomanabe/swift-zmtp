# swift-zmtp(仮の名前)

ZeroMQ を Swift だけで実装する。ZMTP 3.1 を話し、libzmq・pyzmq と通信できることを目指す。
C のライブラリ(libzmq・libsodium)は使わない。調査と計画は [PLAN.md](PLAN.md)。

今は段階 0(下準備)まで。ソケットはまだ無い。

## ビルドとテスト

```bash
swift build
```

```bash
Scripts/test.sh --filter streamsOpenWithZMTP3Greeting
```

`Scripts/test.sh` は `swift test` に引数をそのまま渡す。Xcode が無く Command Line Tools だけの Mac でも Swift Testing が動くように、
フレームワークの場所を足す。Xcode のある Mac や Linux では `swift test` と同じ。

## 相手役(libzmq)

pyzmq 27.2.0(libzmq 4.3.5 を同梱)を [uv](https://docs.astral.sh/uv/) で動かす。どちらのスクリプトも依存をファイルの先頭に書いてあるので、
`uv run` だけで動く。

- `Tools/interop/peer.py`:1 つのソケットを開き、送ったもの・受けたものを 1 行 1 件の JSON で出す。使い方は `--help`
- `Tools/interop/record_fixtures.py`:libzmq 同士のあいだに中継を挟み、両方向のバイト列を `Tests/ZMTPTests/Fixtures/` に書く

```bash
uv run Tools/interop/peer.py --type PULL --bind 'tcp://127.0.0.1:*' --recv 1
```

```bash
uv run Tools/interop/record_fixtures.py --only null-push-pull
```

ポートは 127.0.0.1 の空いているポートを使い、決まった番号は使わない。
