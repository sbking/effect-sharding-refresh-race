# Sharding refresh/acquire race in `effect/cluster`

A minimal reproduction of a race in `effect@4.0.0` where `Sharding` drops shards it has just acquired.

## What goes wrong

Every runner periodically refreshes its shard locks: it asks `RunnerStorage` which of its shards it still holds, and treats any shard missing from the answer as lost.

`refreshShardLocks` sends a snapshot of the shards it holds, but compares the answer against the *current* set. If an acquisition completes while the refresh is in flight, the new shards are missing from the answer only because they were never asked about, and they are released.

```
refresh:  asks about {}  ·········· waiting ··········  answer: {}
acquire:      starts taking {1,2}  ··  done → held = {1,2}

check:    every held shard {1,2} must be in the answer {}  → 1 and 2 "lost", released
```

With a single runner the shards come back on a later acquisition. With several runners rebalancing, the released shards can end up owned by no runner until the next assignment round.

## Reproduce

Requires Node 22.12+ and pnpm 10+.

```sh
pnpm stock   # effect@4.0.0 as published: the test fails
pnpm fixed   # effect@4.0.0 with the fix applied: the test passes
```

Both directories run the same test, [`refresh-race.test.ts`](stock/test/refresh-race.test.ts). It wraps the in-memory `RunnerStorage` so the timing above happens on every run: the first acquisition waits until a refresh has started, and that refresh answers only after the acquisition completes. The test then checks that both shards are still held and that none were released.

Expected output from `pnpm stock`:

```
AssertionError: shard default:1 was dropped: expected false to be true
```

## The fix

Compare the answer only against the shards the refresh asked about, and only for shards still held when the answer arrives:

```ts
const refreshShardLocks = Effect.gen(function*() {
  const refreshed = [...acquiredShards, ...releasingShards]
  const acquired = yield* runnerStorage.refresh(selfAddress, refreshed)
  for (const shardId of refreshed) {
    if (MutableHashSet.has(acquiredShards, shardId) && !acquired.includes(shardId)) {
      MutableHashSet.remove(acquiredShards, shardId)
      MutableHashSet.add(releasingShards, shardId)
    }
  }
  // ...
})
```

Shards acquired in the meantime are checked by the next refresh, well within the lock expiration.

The patch applied in `fixed/` is [`fixed/patches/effect@4.0.0.patch`](fixed/patches/effect@4.0.0.patch). The same change against `main`, with a regression test, is on [sbking/effect `fix/sharding-refresh-acquire-race`](https://github.com/sbking/effect/tree/fix/sharding-refresh-acquire-race) ([compare](https://github.com/Effect-TS/effect/compare/main...sbking:effect:fix/sharding-refresh-acquire-race)).
