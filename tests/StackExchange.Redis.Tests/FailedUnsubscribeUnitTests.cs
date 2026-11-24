using System;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis.Server;
using Xunit;

namespace StackExchange.Redis.Tests;

/// <summary>
/// Unsubscribing a channel's last handler retires its subscription on the client whether or not the server-side
/// unsubscribe succeeds; when the server declined to run it (e.g. <c>BUSY</c>) or it timed out before being declined,
/// the retiring state must be driven by the heartbeat/restore path until the server confirms, otherwise the server
/// stays subscribed (pushing messages that nothing receives) with nothing left to unsubscribe.
/// </summary>
[RunPerProtocol]
public class FailedUnsubscribeUnitTests(ITestOutputHelper log)
{
    private sealed class BusyServer(ITestOutputHelper log) : InProcessTestServer(log)
    {
        private int _refusals;
        private int _pauseFirstUnsubscribe;
        public readonly TaskCompletionSource<bool> FirstUnsubscribe = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public readonly ManualResetEventSlim Release = new(initialState: true);

        /// <summary>Makes the server reply <c>BUSY</c> to the next <paramref name="count"/> unsubscribes.</summary>
        public void RefuseUnsubscribes(int count) => Volatile.Write(ref _refusals, count);

        /// <summary>Pauses the next unsubscribe until <see cref="Release"/> is set, then replies <c>BUSY</c>.</summary>
        public void PauseAndRefuseNextUnsubscribe()
        {
            Release.Reset();
            Volatile.Write(ref _pauseFirstUnsubscribe, 1);
        }

        protected override TypedRedisValue Unsubscribe(RedisClient client, in RedisRequest request)
        {
            if (Interlocked.Exchange(ref _pauseFirstUnsubscribe, 0) != 0)
            {
                FirstUnsubscribe.TrySetResult(true);
                if (!Release.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException("Test unsubscribe barrier timed out.");
                return TypedRedisValue.Error("BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.");
            }

            return Interlocked.Decrement(ref _refusals) >= 0
                ? TypedRedisValue.Error("BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.")
                : base.Unsubscribe(client, request);
        }
    }

    private static async Task<long> PublishUntil(ISubscriber subscriber, RedisChannel channel, Func<long, bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        long received;
        do
        {
            received = await subscriber.PublishAsync(channel, "probe");
            if (condition(received)) break;
            await Task.Delay(50);
        }
        while (DateTime.UtcNow < deadline);
        return received;
    }

    [Fact]
    public async Task DeclinedLastUnsubscribeIsRetried()
    {
        using var server = new BusyServer(log);
        await using var conn = await server.ConnectAsync();
        var subscriber = conn.GetSubscriber();
        var channel = RedisChannel.Literal(nameof(DeclinedLastUnsubscribeIsRetried));
        Action<RedisChannel, RedisValue> handler = static (_, _) => { };

        await subscriber.SubscribeAsync(channel, handler);
        Assert.Equal(1, await subscriber.PublishAsync(channel, "subscribed"));

        server.RefuseUnsubscribes(3);
        var ex = await Assert.ThrowsAsync<RedisServerException>(() => subscriber.UnsubscribeAsync(channel, handler));
        Assert.Equal(RedisErrorKind.Busy, ex.Kind); // the caller still learns that it failed

        // ...but the server ends up unsubscribed nonetheless, once it accepts the unsubscribe
        Assert.Equal(0, await PublishUntil(subscriber, channel, static received => received == 0));
    }

    [Fact]
    public async Task DeclinedFireAndForgetLastUnsubscribeIsRetried()
    {
        using var server = new BusyServer(log);
        await using var conn = await server.ConnectAsync();
        var subscriber = conn.GetSubscriber();
        var channel = RedisChannel.Literal(nameof(DeclinedFireAndForgetLastUnsubscribeIsRetried));
        Action<RedisChannel, RedisValue> handler = static (_, _) => { };

        await subscriber.SubscribeAsync(channel, handler);
        Assert.Equal(1, await subscriber.PublishAsync(channel, "subscribed"));

        // nobody observes the reply of a fire-and-forget command, so the retiring state has to drive it
        server.RefuseUnsubscribes(3);
        await subscriber.UnsubscribeAsync(channel, handler, CommandFlags.FireAndForget);

        Assert.Equal(0, await PublishUntil(subscriber, channel, static received => received == 0));
    }

    [Fact]
    public async Task DeclinedSyncLastUnsubscribeIsRetried()
    {
        using var server = new BusyServer(log);
        await using var conn = await server.ConnectAsync();
        var subscriber = conn.GetSubscriber();
        var channel = RedisChannel.Literal(nameof(DeclinedSyncLastUnsubscribeIsRetried));
        Action<RedisChannel, RedisValue> handler = static (_, _) => { };

        subscriber.Subscribe(channel, handler);
        Assert.Equal(1, await subscriber.PublishAsync(channel, "subscribed"));

        server.RefuseUnsubscribes(3);
        var ex = Assert.Throws<RedisServerException>(() => subscriber.Unsubscribe(channel, handler));
        Assert.Equal(RedisErrorKind.Busy, ex.Kind);

        Assert.Equal(0, await PublishUntil(subscriber, channel, static received => received == 0));
    }

    [Fact]
    public async Task TimedOutThenDeclinedLastUnsubscribeIsRetried()
    {
        using var server = new BusyServer(log);
        var config = server.GetClientConfig();
        config.AsyncTimeout = 200;
        config.HeartbeatInterval = TimeSpan.FromMilliseconds(100);
        await using var conn = await ConnectionMultiplexer.ConnectAsync(config);
        var subscriber = conn.GetSubscriber();
        var channel = RedisChannel.Literal(nameof(TimedOutThenDeclinedLastUnsubscribeIsRetried));
        Action<RedisChannel, RedisValue> handler = static (_, _) => { };

        await subscriber.SubscribeAsync(channel, handler);
        Assert.Equal(1, await subscriber.PublishAsync(channel, "subscribed"));

        server.PauseAndRefuseNextUnsubscribe();
        try
        {
            var unsubscribe = subscriber.UnsubscribeAsync(channel, handler);
            await server.FirstUnsubscribe.Task.WithTimeout(5000);
            await Assert.ThrowsAsync<RedisTimeoutException>(() => unsubscribe.WithTimeout(5000));
            server.Release.Set();

            Assert.Equal(0, await PublishUntil(subscriber, channel, static received => received == 0));
        }
        finally
        {
            server.Release.Set();
        }
    }

    [Fact]
    public async Task RetriedUnsubscribeDoesNotUndoNewerSubscription()
    {
        using var server = new BusyServer(log);
        var config = server.GetClientConfig();
        config.HeartbeatInterval = TimeSpan.FromMilliseconds(100);
        await using var conn = await ConnectionMultiplexer.ConnectAsync(config);
        var subscriber = conn.GetSubscriber();
        var channel = RedisChannel.Literal(nameof(RetriedUnsubscribeDoesNotUndoNewerSubscription));
        Action<RedisChannel, RedisValue> leaving = static (_, _) => { };
        var received = 0;
        Action<RedisChannel, RedisValue> joining = (_, _) => Interlocked.Increment(ref received);

        await subscriber.SubscribeAsync(channel, leaving);
        server.RefuseUnsubscribes(1);
        await Assert.ThrowsAsync<RedisServerException>(() => subscriber.UnsubscribeAsync(channel, leaving));

        // subscribed to again before the retry is due; from then on, the retry must not unsubscribe it
        await subscriber.SubscribeAsync(channel, joining);
        await Task.Delay(TimeSpan.FromMilliseconds(500)); // well past several heartbeats

        Assert.Equal(1, await subscriber.PublishAsync(channel, "still subscribed"));
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (Volatile.Read(ref received) == 0 && DateTime.UtcNow < deadline) await Task.Delay(20);
        Assert.Equal(1, Volatile.Read(ref received));
    }

    [Fact]
    public async Task RestoreSkipsRetiredSubscription()
    {
        using var server = new InProcessTestServer(log);
        await using var conn = await server.ConnectAsync();
        var subscriber = conn.GetSubscriber();
        var channel = RedisChannel.Literal(nameof(RestoreSkipsRetiredSubscription));

        var sub = conn.GetOrAddSubscription(channel, CommandFlags.None);
        sub.Add(static (_, _) => { }, null);
        sub.MarkRetired();

        Assert.Equal(0, conn.EnsureSubscriptions(CommandFlags.None));
        Assert.Equal(0, await subscriber.PublishAsync(channel, "should-not-subscribe"));
    }
}
