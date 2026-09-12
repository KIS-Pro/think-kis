name: KIS Outbox Sync

on:
  schedule:
    - cron: '*/5 * * * *'   # 5分間隔（要調整）
  workflow_dispatch:        # 手動実行用（デバッグ・障害対応時）

concurrency:
  group: kis-outbox-sync
  cancel-in-progress: false  # 実行中のジョブは殺さず、新しい方を待機させる

jobs:
  sync:
    runs-on: ubuntu-latest
    timeout-minutes: 4      # 次回実行(5分後)と重複しないよう余裕を持たせる
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-node@v4
        with:
          node-version: '20'

      - name: Install dependencies
        run: npm ci

      - name: Run outbox worker
        run: node scripts/outbox-worker.js
        env:
          NEON_CONNECTION_STRING: ${{ secrets.NEON_CONNECTION_STRING }}
          NEO4J_URI: ${{ secrets.NEO4J_URI }}
          NEO4J_USER: ${{ secrets.NEO4J_USER }}
          NEO4J_PASSWORD: ${{ secrets.NEO4J_PASSWORD }}
