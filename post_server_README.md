# Post Server API

The Post Server runs on:

```text
http://127.0.0.1:8001
```

All messages use HTTP `POST` requests with JSON.

## Create a Post

The client sends a request to:

```text
POST /posts/create
```

Example request:

```json
{
  "author": 7,
  "content": "photo.jpg",
  "caption": "My first post"
}
```

The Post Server returns:

```json
{
  "success": true,
  "post": {
    "id": 1,
    "author": 7,
    "timestamp": "2026-09-23T15:00:00+00:00",
    "content": "photo.jpg",
    "caption": "My first post",
    "likes": [],
    "comments": []
  },
  "notification_sent": true
}
```

The `id` and `timestamp` are created by the Post Server.

## Retrieve Posts by Author

The Feed Server sends a request to:

```text
POST /posts/retrieve
```

Example request:

```json
{
  "author": 7
}
```

The Post Server returns:

```json
{
  "success": true,
  "posts": [
    {
      "id": 1,
      "author": 7,
      "timestamp": "2026-09-23T15:00:00+00:00",
      "content": "photo.jpg",
      "caption": "My first post",
      "likes": [],
      "comments": []
    }
  ]
}
```

## Notification Server Message

After successfully creating a post, the Post Server sends the Notification Server a request:

```text
POST http://127.0.0.1:8003/notifications
```

Example message:

```json
{
  "type": "new_post",
  "post_id": 1,
  "author": 7,
  "timestamp": "2026-09-23T15:00:00+00:00"
}
```

## Important Notes

* Include this header with every request:

```text
Content-Type: application/json
```

* `author` is the user ID of the post creator.
* `content` is currently the image filename, path, or URL.
* A successful creation returns HTTP status `201`.
* A successful retrieval returns HTTP status `200`.
* Failed requests return `"success": false` and an error message.
