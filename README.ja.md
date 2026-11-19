# junban

[English](README.md) | 日本語

junban は、1台のマシンで計算ジョブを管理するためのツールです。複数のAIエージェントで Kaggle や競技プログラミングをするときに便利です。

- ジョブを、使用するCPU数とGPU数に応じて待ち行列に並べます。空いていれば複数のジョブを同時に実行するので、マシンを無駄なく使えます。
- CPU数を指定しなかったジョブは、ほかのジョブと同時には実行されません。そのため、実行時間を測る評価がほかの計算の影響でブレることがありません。
- コマンドの前に `junban run` を付けるだけで使えます。入出力、環境変数、終了コードは、コマンドを直接実行したときと同じです。

macOS と Ubuntu で動きます。[task-spooler](https://viric.name/soft/ts/) をもとに作りました。

## インストール

Cコンパイラと make が必要です。

```console
$ make
$ make install PREFIX=$HOME/.local
```

`$HOME/.local/bin/junban` がインストールされます。`PREFIX` を省略すると `/usr/local` にインストールします。

## 使い方

### ジョブを実行する

実行したいコマンドの前に `junban run` を付けます。使用するCPU数は `-c` で指定します。

```console
$ junban run -c 4 python train.py
```

`-c` を省略すると、マシンの全CPUを使うジョブとして扱われます。このジョブが動いている間、ほかのジョブは実行されません。実行時間を測る評価は、`-c` を付けずに実行してください。

```console
$ junban run python eval.py
```

### GPUを使うジョブを実行する

ジョブが使うGPU数は `-g`（`--gpus`）で指定します。junban はGPUを1台の装置として扱うので、指定できるのは 0 か 1 です。`-g` を省略すると、GPUを使わないジョブとして扱われます。`-g 1` のジョブは一度に1つしか実行されません。

`-c` と `-g` は、互いの省略時の値を変えません。`-g 1` だけを付けたジョブは全CPUも使うので、ほかのジョブとは同時に実行されません。MPS や MLX を使った学習のように、CPUをあまり使わないGPUジョブには両方を指定してください。そうすれば、CPUジョブを横で同時に実行できます。

```console
$ junban run -c 1 -g 1 python train_mps.py
```

| オプション | CPU | GPU |
|---|---|---|
| なし | 全部 | 0 |
| `-c 4` | 4 | 0 |
| `-c 1 -g 1` | 1 | 1 |
| `-g 1` | 全部 | 1 |

CPUが空いていれば、ジョブはすぐに始まります。空いていなければ、空くまで待ちます。待っている間は、CPUとGPUのどちらを待っているかも含めて、次のように状況が表示されます。

```console
$ junban run python eval.py
junban: id=1 queued, cpus=all(4), gpus=0, 1 ahead, waiting for cpus
junban:   running id=0 dir=demo cpus=1 gpus=0 elapsed=0s est=30m
junban: id=1 started after 1s
```

この表示は標準エラーに出ます。標準出力には、コマンド自身の出力だけが流れます。

### バックグラウンドで実行する

`junban run` は、ジョブが終わるまで戻りません。複数のジョブを並列に実行したいときは `--detach` を使います。ジョブを待ち行列に入れ、標準出力にジョブIDだけを出して、すぐに戻ります。

```console
$ id=$(junban run --detach -c 8 python train.py)
junban: id=63 log=/Users/me/Library/Caches/junban/queue.sock.logs/63.log
```

ジョブはその後、順番を待って通常どおり実行されます。ジョブの標準出力と標準エラーは、待ち行列のディレクトリ（macOS では `~/Library/Caches/junban`、Ubuntu では `$XDG_RUNTIME_DIR/junban` か `~/.cache/junban`）の `queue.sock.logs/ID.log` に書かれます。このパスは `junban ls` にも `log=` として表示されます。標準入力は `/dev/null` です。終了を待つには、次に説明する `junban wait` を使います。junban のサーバーが再起動するとジョブIDは0から振り直され、同じIDの古いログは新しいジョブで上書きされます。

### ジョブの終了を待つ

`junban wait` は、指定したジョブがすべて終わるまで待ちます。終了コードは各ジョブの終了コードの最大値なので、すべて成功すれば 0 です。

```console
$ a=$(junban run --detach -c 4 python train.py --fold 0)
$ b=$(junban run --detach -c 4 python train.py --fold 1)
$ junban wait "$a" "$b"
```

- すでに終わったジョブは、その結果からすぐに判定します。junban は、終わったジョブのうち最後の1000個の結果を覚えています。
- `--timeout DUR` を付けると、`DUR`（`90s`、`30m`、`2h`、`1d`）が過ぎたところで待つのをやめ、終了コード 124 を返します。ジョブは動き続けます。
- `--tag TAG` を付けると、待ち始めた時点で実行中・待機中のそのタグのジョブと、待っている間に投入されたそのタグのジョブを待ちます。待ち始める前に終わっていたジョブは含みません。ジョブごとの結果を確実に知りたいときは、IDを指定して待ちます。
- 既定では何も出力しません。`-v` を付けると、ジョブが終わるたびに標準出力に1行出します（例: `finished id=63 exit=0 elapsed=4m12s`）。
- junban 自身のエラー（存在しないジョブID、待ち行列に接続できない、など）のときは 125 を返します。

ジョブを待つときは `junban wait` を使い、`junban ls` を見張るループは書かないでください。一覧全体を見張ると、ほかのエージェントのジョブまで待ってしまいます。

### かかる時間を伝える

ジョブにかかる時間の見込みを `--estimate` で伝えられます。`90s`、`30m`、`2h`、`1d` のように単位を付けて書きます。

```console
$ junban run -c 8 --estimate 2h python train.py
```

見込みは、待っているほかのジョブに表示されます。見込みを過ぎても、ジョブが打ち切られることはありません。

### ジョブにタグを付ける

`--tag` でジョブにタグを付けると、自分のジョブをまとめて扱えます。環境変数 `JUNBAN_TAG` を設定しておくと、`--tag` を省略してもそのタグが付きます。エージェントのセッションごとに一度設定しておけば、そのエージェントのジョブが自然にまとまります。タグには `A-Z a-z 0-9 . _ : @ + -` の文字を1〜63文字使えます。

```console
$ export JUNBAN_TAG=agent-1
$ junban run -c 4 python train.py
$ junban ls --tag agent-1
running id=5 dir=demo tag=agent-1 cpus=4 gpus=0 elapsed=10s cmd=python train.py
$ junban kill --tag agent-1
```

`junban kill --tag TAG` は、そのタグが付いた実行中・待機中のジョブをすべて止めます。該当するジョブがないときは終了コード 1 を返します。

### 状況を確認する

`junban ls` で、実行中のジョブと待っているジョブを1行に1つずつ確認できます。

```console
$ junban ls
running id=0 dir=demo cpus=1 gpus=0 elapsed=1s est=30m cmd=sleep 30
queued id=1 dir=demo cpus=all(4) gpus=0 waited=1s est=5m cmd=sh -c echo evaluated
```

`dir` は、ジョブを投入したディレクトリの名前です。`cpus=all(4)` は、`-c` を省略したジョブです。`gpus=` は `-g` で指定したGPU数で、省略したときは 0 です。見込みの時間を過ぎたジョブには `over` と表示されます。終わったジョブも見たいときは `junban ls -a` を使います。終わったジョブの行は `done` で始まります。

`junban ls` の標準出力には、ジョブの行だけが出ます。どの行も、ジョブの状態（`running`、`queued`、`done`）で始まります。ジョブがないときは何も出力しません。終了コードは `grep` と同じく、ジョブを1件以上表示したときは 0、1件もなかったときは 1、junban 自身のエラーのときは 125 です。CPUとGPUの使用状況を見たいときは `junban ls --summary` を使います。

```console
$ junban ls --summary
cpus 1/4 gpus 0/1 running 1 queued 1
```

一部のジョブだけを表示したいときは、ジョブIDを `--id N` で、ディレクトリ（`dir=` の名前）を `--dir NAME` で、タグを `--tag TAG` で指定します。どちらも繰り返し指定でき、互いに、また `-a` とも組み合わせられます。このときの終了コードは、指定したジョブが表示されたかどうかを表します。

```console
$ junban ls --id 63 --id 64
running id=63 dir=demo cpus=8 gpus=0 elapsed=5m cmd=python train.py
$ junban ls -a --dir demo
```

人間が見るときは `junban watch` が便利です。画面が2秒ごとに更新されます。`q` で終了します。

```text
junban                                                         14:32:05  q:quit
 CPU ████████████████████████████████████░░░░░░░░░░░░  3/4   running 2
 GPU ████████████████████████████████████████████████  1/1   queued  1

RUNNING 2
  1  python train.py --lr 0.1 --epochs 50
     AHC075-b/  12m / 30m ━━━━━━━──────────── 18m left  cpus 2 gpus 1  #sweep
  3  sleep 30
     AHC075-a/  45s / 30s ━━━━━━━━━━━━━━━━━━ OVER +15s  cpus 1

QUEUED 1
  4  python eval.py --ckpt best.pt
     AHC075-b/  waited 40s  est 10m                     cpus 1 gpus 1  #sweep

DONE
✗ 2  pytest -x
     AHC075-a/  exit 1   42s  ended 14:25
✓ 0  true
     AHC075-a/  ok       0s  ended 14:20
```

ジョブ1件は2行で表示されます。1行目はIDとコマンド、2行目はディレクトリ、時間、使っているCPUとGPU、タグです。ディレクトリの欄は、画面にある名前が収まる幅まで広がります（最大32桁）。見積もりのある実行中ジョブには進捗バーと残り時間が出て、見積もりを超えると `OVER` になります。待ち中のジョブには見積もりが出ます。終わったジョブには `✓` か `✗`、結果、終了時刻が出ます。色は意味を表すときだけ使います。CPUはシアン、GPUはマゼンタ、ディレクトリは青、異常は赤です。`NO_COLOR=1` で色を消せます。UTF-8のロケールでなければASCIIだけで表示し、ジョブの文字列に含まれる非ASCII文字は1文字ずつ `?` にします。幅の狭い端末では、まずタグを省き、次に時間の欄を縮めます。どの項目も、途中で切らずに全部出すか省くかのどちらかです。

### ジョブを止める

`junban kill` にジョブのIDを渡します。実行中のジョブは停止し、待っているジョブは待ち行列から取り除かれます。

```console
$ junban kill 0
```

`junban run` を Ctrl-C などで中断した場合も、実行中のコマンドは一緒に止まります。

## 知っておくとよいこと

- ジョブは、CPUとGPUのそれぞれについて投入した順に始まります。待っているジョブは、自分に必要なCPUとGPUを押さえます。後ろのジョブは、その残りに収まるときだけ始まります。そのため、GPUを待つジョブの後ろにいるCPUジョブは先に始まれますが、前のジョブが待っているCPUを使うことはありません。全CPUを使うジョブが待っていれば全CPUが押さえられるので、後ろのジョブが先に始まることはありません。小さいジョブが先に実行され続けて、大きいジョブがいつまでも始まらない、ということが起きないようにするためです。
- `junban run a && b` と書くと、待ち行列に入るのは `a` だけです。まとめて入れたいときは `junban run sh -c 'a && b'` と書きます。
- junban で実行しているジョブの中から `junban run` を呼ぶことはできません。呼ぶとエラー（終了コード 125）になります。ジョブが自分自身の終了を待ち続けて、待ち行列が止まるのを防ぐためです。
- 子プロセスをバックグラウンドに残して終了するコマンドでは、子プロセスが終わるまで `junban run` も終了しません。
- `junban run` の終了コードは、コマンドの終了コードです。junban 自身のエラー（待ち行列に接続できない、引数が正しくない、など）のときは 125 を返します。
- ジョブが残っている間、macOS はスリープしません。

## AIエージェントへの指示

`AGENTS.md` や `CLAUDE.md` に、次のように書いておきます。

```text
- 時間のかかる計算は、必ず junban run を付けて実行する。
- 使用するCPU数を -c で正しく指定する。スレッドや子プロセスの分も含める。
- GPUを使うジョブには -g 1 を付ける。-c も合わせて指定する（例：-c 1 -g 1）。
- 実行時間を測る評価は、-c を付けずに実行する。
- かかる時間の見当がつくときは --estimate で伝える。
- junban run を前景で実行すれば、ジョブの終了まで戻らない。並列にするときは、
  id=$(junban run --detach ...) で受け取ったIDを junban wait に渡して待つ。
  junban wait の終了コードは、各ジョブの終了コードの最大値である。
- junban ls の出力を grep して待つループは書かない。
- ほかのエージェントのジョブは止めない。
```

エージェントのセッションごとに `JUNBAN_TAG` を設定する場合（例: `export JUNBAN_TAG=agent-1`）は、「自分のジョブをまとめて止めるときは junban kill --tag "$JUNBAN_TAG" を使う。」のような1行を加えてもよいです。

## 制限

- 1台のマシンを1人で使うことを想定しています。ほかのユーザーのジョブとは調整しません。
- junban を通さずに実行した計算は管理できません。評価と同時に動いても防げません。
- `-c` と `-g` で指定したCPU数とGPU数は自己申告です。指定より多く使っても制限されません。メモリは管理しません。
- GPUは1台の装置として管理します。junban はGPUを検出せず、ジョブにGPUを割り当てることもしません。そのため、GPUが複数あるマシンでも `-g 1` のジョブは1つずつ実行されます。
- junban のサーバーが止まると、待っていたジョブはエラー（終了コード 125）で終了します。実行中のコマンドはそのまま動き続けます。
- 同時に扱えるジョブは、実行中と待機中を合わせて995個までです。
- サンドボックスの設定によっては、junban のサーバーに接続できないことがあります。その場合はエラーになり、コマンドは実行されません。

## 動作確認

`make test` でテストを実行できます（Python 3 が必要です）。次の環境で確認しています。

- macOS（Apple Silicon）
- Ubuntu 24.04（arm64、Docker 上）

## ライセンス

junban は、Lluís Batlle i Rossell 氏の [task-spooler](https://viric.name/soft/ts/) 1.0.4 を改変したものです。ライセンスは GNU General Public License version 2 です。全文は [COPYING](COPYING) にあります。
