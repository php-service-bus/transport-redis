<?php

/**
 * Redis transport implementation.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Transport\Redis\Tests;

use function Amp\Promise\wait;
use function ServiceBus\Common\uuid;
use Amp\Loop;
use PHPUnit\Framework\TestCase;
use ServiceBus\Transport\Common\Package\OutboundPackage;
use ServiceBus\Transport\Redis\RedisChannel;
use ServiceBus\Transport\Redis\RedisConsumer;
use ServiceBus\Transport\Redis\RedisIncomingPackage;
use ServiceBus\Transport\Redis\RedisPublisher;
use ServiceBus\Transport\Redis\RedisTransportConnectionConfiguration;
use ServiceBus\Transport\Redis\RedisTransportLevelDestination;

/**
 *
 */
final class RedisConsumerTest extends TestCase
{
    private RedisTransportConnectionConfiguration $config;

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    protected function setUp(): void
    {
        parent::setUp();

        $this->config = new RedisTransportConnectionConfiguration((string) \getenv('REDIS_CONNECTION_DSN'));
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    protected function tearDown(): void
    {
        parent::tearDown();

        unset($this->config);
    }

    /**
     * @test
     *
     * @throws \Throwable
     */
    public function pubSub(): void
    {
        $consumer  = new RedisConsumer(new RedisChannel('qwerty'), $this->config);
        $publisher = new RedisPublisher($this->config);

        Loop::run(
            static function() use ($publisher, $consumer)
            {
                $consumer->listen(
                    static function(RedisIncomingPackage $package) use ($consumer): \Generator
                    {
                        /** @var RedisTransportLevelDestination $destination */
                        $destination = $package->origin();

                        static::assertSame('qwerty', $package->payload());
                        static::assertSame('qwerty', $destination->channel);

                        yield $consumer->stop();

                        Loop::stop();
                    }
                );

                yield $publisher->publish(
                    new OutboundPackage(
                        'qwerty',
                        [],
                        new RedisTransportLevelDestination('qwerty'),
                        uuid()
                    )
                );
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     */
    public function disconnectWithoutConsuming(): void
    {
        $consumer = new RedisConsumer(new RedisChannel('qwerty'), $this->config);
        wait($consumer->stop());
    }
}
