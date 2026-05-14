# PokeReto Server

令和版ポケベルアプリ「PokeReto」のWebSocketサーバー（クラウド対応版）

## Features

- WebSocket 通信
- メッセージ履歴の DB 保存（PostgreSQL）
- オフラインメッセージ対応（受信者が後で見られる）
- ヘルスチェックエンドポイント

## Local Development

```bash
pip install -r requirements.txt
python3 server.py
```

## Deployment on Render.com

1. Create a new Web Service
2. Connect this GitHub repository
3. Add a PostgreSQL database
4. Set DATABASE_URL environment variable
5. Deploy

## Environment Variables

- `PORT` - サーバーポート (default: 8080)
- `DATABASE_URL` - PostgreSQL 接続URL (Render が自動設定)

## License

MIT
