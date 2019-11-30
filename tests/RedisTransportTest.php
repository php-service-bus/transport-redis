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
use Ramsey\Uuid\Uuid;
use ServiceBus\Transport\Common\Exceptions\ConnectionFail;
use ServiceBus\Transport\Common\Package\OutboundPackage;
use ServiceBus\Transport\Common\Transport;
use ServiceBus\Transport\Redis\RedisChannel;
use ServiceBus\Transport\Redis\RedisIncomingPackage;
use ServiceBus\Transport\Redis\RedisTransport;
use ServiceBus\Transport\Redis\RedisTransportConnectionConfiguration;
use ServiceBus\Transport\Redis\RedisTransportLevelDestination;

/**
 *
 */
final class RedisTransportTest extends TestCase
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
    public function flow(): void
    {
        $transport = new RedisTransport($this->config, null);

        Loop::run(
            static function() use ($transport): \Generator
            {
                $messages = [];

                yield $transport->consume(
                    static function(RedisIncomingPackage $message) use (&$messages, $transport): \Generator
                    {
                        static::assertInstanceOf(RedisIncomingPackage::class, $message);
                        static::assertTrue(Uuid::isValid($message->id()));
                        static::assertTrue(Uuid::isValid($message->traceId()));
                        static::assertArrayHasKey(Transport::SERVICE_BUS_TRACE_HEADER, $message->headers());
                        static::assertTrue(Uuid::isValid($message->headers()[Transport::SERVICE_BUS_TRACE_HEADER]));

                        $messages[] = $message->payload();

                        if (2 === \count($messages))
                        {
                            static::assertSame(['qwerty.message', 'root.message'], $messages);

                            yield $transport->stop();

                            Loop::stop();
                        }
                    },
                    new RedisChannel('qwerty'),
                    new  RedisChannel('root')
                );

                yield $transport->send(
                    new  OutboundPackage('qwerty.message', [], new RedisTransportLevelDestination('qwerty'), uuid())
                );

                yield $transport->send(
                    new OutboundPackage('root.message', [], new RedisTransportLevelDestination('root'), uuid())
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
    public function subscribeWithWrongConnectionData(): void
    {
        $this->expectException(ConnectionFail::class);
        $this->expectExceptionMessage('Failed to connect to redis instance (tcp://localhost:1000)');

        $config = new RedisTransportConnectionConfiguration('tcp://localhost:1000');

        Loop::run(
            static function() use ($config)
            {
                $transport = new RedisTransport($config);

                yield $transport->consume(
                    static function(): void
                    {
                    },
                    new  RedisChannel('root')
                );
            }
        );
    }

    /**
     * @test
     *
     * @throws \Throwable
     */
    public function disconnectWithoutConnections(): void
    {
        wait((new RedisTransport($this->config))->disconnect());
    }
}
