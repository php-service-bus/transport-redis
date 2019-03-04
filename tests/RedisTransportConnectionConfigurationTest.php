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

use PHPUnit\Framework\TestCase;
use ServiceBus\Transport\Redis\Exceptions\IncorrectConnectionParameters;
use ServiceBus\Transport\Redis\RedisTransportConnectionConfiguration;

/**
 *
 */
final class RedisTransportConnectionConfigurationTest extends TestCase
{
    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     *
     */
    public function successCreate(): void
    {
        $config = new RedisTransportConnectionConfiguration('tcp://test-host:7000?timeout=-1&password=qwerty');

        static::assertSame('test-host', $config->host);
        static::assertSame(7000, $config->port);
        static::assertSame(5, $config->timeout);
        static::assertSame('qwerty', $config->password);
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     *
     */
    public function emptyDSN(): void
    {
        $this->expectException(IncorrectConnectionParameters::class);
        $this->expectExceptionMessage('Connection DSN can\'t be empty');

        new RedisTransportConnectionConfiguration('');
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     *
     */
    public function withoutScheme(): void
    {
        $this->expectException(IncorrectConnectionParameters::class);
        $this->expectExceptionMessage('Connection DSN must start with tcp:// or unix://');

        new RedisTransportConnectionConfiguration('test-host:7000');
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     *
     */
    public function incorrectDSN(): void
    {
        $this->expectException(IncorrectConnectionParameters::class);
        $this->expectExceptionMessage('Can\'t parse specified connection DSN (tcp:///example.org:80)');

        new RedisTransportConnectionConfiguration('tcp:///example.org:80');
    }
}
