# junban

[English](README.md) | 日本語

junban は、1台のマシンで計算ジョブを管理するためのツールです。複数のAIエージェントで Kaggle や競技プログラミングをするときに便利です。

- ジョブを、使用するCPU数に応じて待ち行列に並べます。CPUが空いていれば複数のジョブを同時に実行するので、マシンを無駄なく使えます。
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

CPUが空いていれば、ジョブはすぐに始まります。空いていなければ、空くまで待ちます。待っている間は、次のように状況が表示されます。

```console
$ junban run python eval.py
junban: id=1 queued, cpus=all(4), 1 ahead
junban:   running id=0 dir=demo cpus=1 elapsed=0s est=30m
junban: id=1 started after 1s
```

この表示は標準エラーに出ます。標準出力には、コマンド自身の出力だけが流れます。

### バックグラウンドで実行する

`junban run` は、ジョブが終わるまで戻りません。複数のジョブを並列に実行したいときは `--detach` を使います。ジョブを待ち行列に入れ、標準出力にジョブIDだけを出して、すぐに戻ります。

```console
$ id=$(junban run --detach -c 8 python train.py)
junban: id=63 log=/Users/me/Library/Caches/junban/logs/63.log
```

ジョブはその後、順番を待って通常どおり実行されます。ジョブの標準出力と標準エラーは、待ち行列のディレクトリ（macOS では `~/Library/Caches/junban`、Ubuntu では `$XDG_RUNTIME_DIR/junban` か `~/.cache/junban`）の `logs/ID.log` に書かれます。このパスは `junban ls` にも `log=` として表示されます。標準入力は `/dev/null` です。終わったあとは、`junban ls -a --id 63` でどう終了したかを確認できます。junban のサーバーが再起動するとジョブIDは0から振り直され、同じIDの古いログは新しいジョブで上書きされます。

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
running id=5 dir=demo tag=agent-1 cpus=4 elapsed=10s cmd=python train.py
$ junban kill --tag agent-1
```

`junban kill --tag TAG` は、そのタグが付いた実行中・待機中のジョブをすべて止めます。該当するジョブがないときは終了コード 1 を返します。

### 状況を確認する

`junban ls` で、実行中のジョブと待っているジョブを1行に1つずつ確認できます。

```console
$ junban ls
running id=0 dir=demo cpus=1 elapsed=1s est=30m cmd=sleep 30
queued id=1 dir=demo cpus=all(4) waited=1s est=5m cmd=sh -c echo evaluated
```

`dir` は、ジョブを投入したディレクトリの名前です。`cpus=all(4)` は、`-c` を省略したジョブです。見込みの時間を過ぎたジョブには `over` と表示されます。終わったジョブも見たいときは `junban ls -a` を使います。終わったジョブの行は `done` で始まります。

`junban ls` の標準出力には、ジョブの行だけが出ます。どの行も、ジョブの状態（`running`、`queued`、`done`）で始まります。ジョブがないときは何も出力しません。終了コードは `grep` と同じく、ジョブを1件以上表示したときは 0、1件もなかったときは 1、junban 自身のエラーのときは 125 です。CPUの使用状況を見たいときは `junban ls --summary` を使います。

```console
$ junban ls --summary
cpus 1/4 running 1 queued 1
```

一部のジョブだけを表示したいときは、ジョブIDを `--id N` で、ディレクトリ（`dir=` の名前）を `--dir NAME` で、タグを `--tag TAG` で指定します。どちらも繰り返し指定でき、互いに、また `-a` とも組み合わせられます。このときの終了コードは、指定したジョブが表示されたかどうかを表します。

```console
$ junban ls --id 63 --id 64
running id=63 dir=demo cpus=8 elapsed=5m cmd=python train.py
$ junban ls -a --dir demo
```

人間が見るときは `junban watch` が便利です。画面が2秒ごとに更新されます。`q` で終了します。

```text
junban  cpus 3/4 [#########################---------] running 1 queued 1 q:quit
RUNNING
 ID DIR              CPUS ELAPSED EST      COMMAND
  1 AHC075-b            3 1s      1s OVER  sleep 30
QUEUED
 ID DIR              CPUS WAITED  EST      COMMAND
  2 AHC075-a          all 1s      5m       true
DONE
 ID DIR              CPUS ELAPSED RESULT      COMMAND
  0 AHC075-a          all 0s      exit=0      true
```

### ジョブを止める

`junban kill` にジョブのIDを渡します。実行中のジョブは停止し、待っているジョブは待ち行列から取り除かれます。

```console
$ junban kill 0
```

`junban run` を Ctrl-C などで中断した場合も、実行中のコマンドは一緒に止まります。

## 知っておくとよいこと

- ジョブは投入した順に始まります。先頭のジョブに必要なCPUが空くまで、後ろのジョブは始まりません。小さいジョブが先に実行され続けて、大きいジョブがいつまでも始まらない、ということが起きないようにするためです。
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
- 実行時間を測る評価は、-c を付けずに実行する。
- かかる時間の見当がつくときは --estimate で伝える。
- 待たされたときは junban ls で状況を確認する。ほかのエージェントのジョブは止めない。
```

## 制限

- 1台のマシンを1人で使うことを想定しています。ほかのユーザーのジョブとは調整しません。
- junban を通さずに実行した計算は管理できません。評価と同時に動いても防げません。
- `-c` で指定したCPU数は自己申告です。指定より多く使っても制限されません。メモリとGPUは管理しません。
- junban のサーバーが止まると、待っていたジョブはエラー（終了コード 125）で終了します。実行中のコマンドはそのまま動き続けます。
- 同時に扱えるジョブは、実行中と待機中を合わせて995個までです。
- サンドボックスの設定によっては、junban のサーバーに接続できないことがあります。その場合はエラーになり、コマンドは実行されません。

## 動作確認

`make test` でテストを実行できます（Python 3 が必要です）。次の環境で確認しています。

- macOS（Apple Silicon）
- Ubuntu 24.04（arm64、Docker 上）

## ライセンス

junban は、Lluís Batlle i Rossell 氏の [task-spooler](https://viric.name/soft/ts/) 1.0.4 を改変したものです。ライセンスは GNU General Public License version 2 です。全文は [COPYING](COPYING) にあります。
