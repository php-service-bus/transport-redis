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
    public function flow(): void
    {
        $transport = new RedisTransport($this->config, null, false);

        Loop::run(
            function() use ($transport): \Generator
            {
                $messages = [];

                yield $transport->consume(
                    function(RedisIncomingPackage $message) use (&$messages, $transport): \Generator
                    {
                        static::assertGreaterThan(0, $message->time());
                        static::assertInstanceOf(RedisIncomingPackage::class, $message);
                        static::assertTrue(Uuid::isValid($message->id()));
                        static::assertTrue(Uuid::isValid($message->traceId()));
                        static::assertArrayHasKey(Transport::SERVICE_BUS_TRACE_HEADER, $message->headers());
                        static::assertTrue(Uuid::isValid($message->headers()[Transport::SERVICE_BUS_TRACE_HEADER]));

                        $messages[] = $message->payload();

                        if(2 === \count($messages))
                        {
                            static::assertSame(['qwerty.message', 'root.message'], $messages);

                            yield $transport->stop();

                            Loop::stop();
                        }
                    },
                    RedisChannel::create('qwerty'),
                    RedisChannel::create('root')
                );

                yield $transport->send(
                    OutboundPackage::create('qwerty.message', [], new RedisTransportLevelDestination('qwerty'), uuid())
                );

                yield $transport->send(
                    OutboundPackage::create('root.message', [], new RedisTransportLevelDestination('root'), uuid())
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
        $this->expectExceptionMessage('Connection attempt failed');

        $config = new RedisTransportConnectionConfiguration('tcp://localhost:9000');

        Loop::run(
            function() use ($config)
            {
                $transport = new RedisTransport($config);

                yield $transport->consume(
                    function(): void
                    {

                    },
                    RedisChannel::create('root')
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
    public function disconnectWithoutConnections(): void
    {
        wait((new RedisTransport($this->config))->disconnect());
    }
}
