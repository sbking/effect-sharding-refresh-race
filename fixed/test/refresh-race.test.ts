import { assert, it } from "@effect/vitest"
import { Context, Deferred, Effect, Layer, Option } from "effect"
import {
  MessageStorage,
  RunnerAddress,
  RunnerHealth,
  Runners,
  RunnerStorage,
  ShardId,
  Sharding,
  ShardingConfig
} from "effect/cluster"

// A runner acquires its shards while a shard lock refresh is in flight. The
// refresh only asked about the shards held before the acquisition, so its
// result cannot mention the new ones. Sharding must not treat them as lost.
it.live("keeps shards acquired while a lock refresh is in flight", () =>
  Effect.gen(function*() {
    const held = new Set<string>()
    const released: Array<string> = []
    const acquireStarted = yield* Deferred.make<void>()
    const refreshStarted = yield* Deferred.make<void>()
    const acquireDone = yield* Deferred.make<void>()

    const memory = yield* RunnerStorage.makeMemory
    const storage = RunnerStorage.RunnerStorage.of({
      ...memory,
      // The first acquisition completes only after a refresh has started.
      acquire: (_address, shardIds) =>
        Effect.gen(function*() {
          const ids = Array.from(shardIds)
          if (!(yield* Deferred.isDone(acquireDone))) {
            yield* Deferred.succeed(acquireStarted, undefined)
            yield* Deferred.await(refreshStarted)
          }
          for (const id of ids) held.add(id.toString())
          yield* Deferred.succeed(acquireDone, undefined)
          return ids
        }),
      // That refresh answers only after the acquisition has completed.
      refresh: (_address, shardIds) =>
        Effect.gen(function*() {
          const ids = Array.from(shardIds)
          if ((yield* Deferred.isDone(acquireStarted)) && !(yield* Deferred.isDone(acquireDone))) {
            yield* Deferred.succeed(refreshStarted, undefined)
            yield* Deferred.await(acquireDone)
          }
          return ids.filter((id) => held.has(id.toString()))
        }),
      release: (_address, shardId) =>
        Effect.sync(() => {
          released.push(shardId.toString())
          held.delete(shardId.toString())
        })
    })

    const context = yield* Layer.build(
      Sharding.layer.pipe(
        Layer.provide(Runners.layerNoop),
        Layer.provide([
          Layer.succeed(RunnerStorage.RunnerStorage, storage),
          RunnerHealth.layerNoop,
          MessageStorage.layerNoop
        ]),
        Layer.provide(
          ShardingConfig.layer({
            runnerAddress: Option.some(RunnerAddress.make("localhost", 34431)),
            shardsPerGroup: 2,
            shardLockRefreshInterval: "100 millis",
            shardLockExpiration: "2 seconds"
          })
        )
      )
    )
    const sharding = Context.get(context, Sharding.Sharding)

    yield* Deferred.await(acquireDone)
    // Give the overlapping refresh and several later ones time to finish.
    yield* Effect.sleep("1 second")

    assert.isTrue(sharding.hasShardId(ShardId.make("default", 1)), "shard default:1 was dropped")
    assert.isTrue(sharding.hasShardId(ShardId.make("default", 2)), "shard default:2 was dropped")
    assert.deepStrictEqual(released, [])
  }), 10_000)
