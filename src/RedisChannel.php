<?php

/**
 * Redis transport implementation.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Transport\Redis;

use ServiceBus\Transport\Common\Queue;
use ServiceBus\Transport\Redis\Exceptions\IncorrectChannelName;

/**
 *
 */
final class RedisChannel implements Queue
{
    /**
     * @var string
     */
    private $channel;

    /**
     * @param string $channel
     *
     * @throws \ServiceBus\Transport\Redis\Exceptions\IncorrectChannelName
     *
     * @return self
     *
     */
    public static function create(string $channel): self
    {
        return new self($channel);
    }

    /**
     * {@inheritdoc}
     */
    public function __toString(): string
    {
        return $this->channel;
    }

    /**
     * @param string $channel
     *
     * @throws \ServiceBus\Transport\Redis\Exceptions\IncorrectChannelName
     */
    private function __construct(string $channel)
    {
        if ('' === $channel)
        {
            throw IncorrectChannelName::emptyChannelName();
        }

        $this->channel = $channel;
    }
}
