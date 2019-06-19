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
use Amp\Redis\Client;
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
    /**
     * @var RedisTransportConnectionConfiguration
     */
    private $config;

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
     *
     * @return void
     *
     */
    public function pubSub(): void
    {
        $consumer  = new RedisConsumer(RedisChannel::create('qwerty'), $this->config);
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
     *
     * @return void
     *
     */
    public function pubSubWithSimpleMessageBody(): void
    {
        $consumer      = new RedisConsumer(RedisChannel::create('qwerty'), $this->config);
        $publishClient = new Client((string) $this->config);

        Loop::run(
            static function() use ($consumer, $publishClient): \Generator
            {
                $consumer->listen(
                    static function(RedisIncomingPackage $package) use ($consumer): \Generator
                    {
                        static::assertSame('simpleBody', $package->payload());

                        yield $consumer->stop();

                        Loop::stop();
                    }
                );

                yield $publishClient->publish('qwerty', 'simpleBody');
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     *
     */
    public function disconnectWithoutConsuming(): void
    {
        $consumer = new RedisConsumer(RedisChannel::create('qwerty'), $this->config);
        wait($consumer->stop());
    }
}
