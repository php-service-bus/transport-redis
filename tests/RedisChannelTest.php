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
use ServiceBus\Transport\Redis\Exceptions\IncorrectChannelName;
use ServiceBus\Transport\Redis\RedisChannel;

/**
 *
 */
final class RedisChannelTest extends TestCase
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
        static::assertSame('qwerty', RedisChannel::create('qwerty')->name);
    }

    /**
     * @test
     *
     * @throws \Throwable
     *
     * @return void
     *
     */
    public function createWithEmptyName(): void
    {
        $this->expectException(IncorrectChannelName::class);

        RedisChannel::create('');
    }
}
