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
use PHPUnit\Framework\TestCase;
use ServiceBus\Transport\Redis\RedisChannel;
use ServiceBus\Transport\Redis\RedisConsumer;
use ServiceBus\Transport\Redis\RedisTransportConnectionConfiguration;

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
    public function disconnectWithoutConsuming(): void
    {
        $consumer = new RedisConsumer(new RedisChannel('qwerty'), $this->config);
        wait($consumer->stop());
    }
}
