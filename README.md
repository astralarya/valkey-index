# valkey-index

Strongly typed index abstractions for Valkey


## Example

Install from [npm](https://www.npmjs.com/package/valkey-index)

```bash
npm install valkey-index
```

Create an index:

```ts
import Valkey from "iovalkey";
import {
  createValkeyIndex,
  getHash,
  setHash,
  updateHash,
} from "valkey-index";

const valkey = new Valkey();

type Session = {
  id: string,
  expires: Date,
  user_id: string,
}

const sessionIndex = createValkeyIndex(
  {
    valkey,
    name: "session",
    exemplar: 0 as Session | 0,
    relations: ["user_id"],
    get: getHash(),
    set: setHash(),
    update: updateHash(),
  },
);

// Add a session (strongly typed!)
const tomorrow = new Date();
tomorrow.setUTCDate(tomorrow.getUTCDate() + 1);
await sessionIndex.set({
  pkey: 1,
  input: {
    id: 1,
    expires: tomorrow,
    user_id: 99,
  }
});

// Find user by pkey or via user_id (strongly typed!)
const session = await sessionIndex.get({ pkey: 1 })
const user_session = await sessionIndex.get({ fkey: 99, relation: "user_id" })

// Subscribe to a session (AsyncGenerator)
const session_subscription = sessionIndex.subscribe({ pkey: 1 })
const user_session_subscription = sessionIndex.subscribe({ fkey: 99, relation: "user_id" })
```


## Usage

Index and relation names may use alphanumerics, underscore, dots, and forward slash (`[a-zA-Z0-9_\.\/]`).
For example, these are all valid names:
* `auth.users`
* `auth.users/chats`
* `auth.session_metadata/notifications...`

Relations are joined to index names with the `@` symbol:
* `session@user`

Keys are joined to indexes with the `:` symbol:
* `auth.users:alice`
* `auth.session@user:bob`


## Developing

You can run a local Valkey instance with the `./start-valkey.sh` script.
Set the `VKPORT` environment variable to change the port (default 6379).