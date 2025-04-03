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
  ValkeyHashIndex,
  ValkeyType,
} from "valkey-index";

const valkey = new Valkey();

type Session = {
  id: number,
  expires?: Date,
  user_id: number,
}

const sessionIndex = ValkeyHashIndex(
  {
    valkey,
    name: "session",
    type: ValkeyType<Session>(),
    relations: ["user_id"],
  },
);

// Add a session (strongly typed!)
await sessionIndex.set({
  pkey: 1,
  input: {
    id: 1,
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

Index and relation names may use alphanumerics, underscore, dots, and forward slash (`[a-zA-Z0-9_./]`).
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

Dependencies:
* podman
* bun

You can run a local Valkey instance with the `./start-valkey.sh` script.
Set the `VKPORT` environment variable to change the port (default 6379).

When running tests, use `bun run test`, rather than the short form.
Alternatively, execute `./run-tests.sh` directly.
Otherwise, the test Valkey instance will not start for the tests.