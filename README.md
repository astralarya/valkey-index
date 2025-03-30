# valkey-index

Strongly typed index abstractions for Valkey


## Example

Create an index:

```ts
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

// Find user by pkey or via user_id (relation is typed!)
const session = await sessionIndex.get({ pkey: 1 })
const user_session = await sessionIndex.get({ fkey: 99, relation: "user_id" })

// Subscribe to a session (returns AsyncGenerators)
const session_subscription= sessionIndex.subscribe({ pkey: 1 })
const user_session_subscription= sessionIndex.subscribe({ fkey: 1, relation: "user_id" })
```