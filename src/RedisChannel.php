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
 * Channel.
 *
 * @property-read string $name
 */
final class RedisChannel implements Queue
{
    /**
     * Channel name.
     *
     * @var string
     */
    public $name;

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
     * @deprecated Will be removed in the next version (use toString() method)
     *
     * {@inheritdoc}
     */
    public function __toString(): string
    {
        return $this->name;
    }

    /**
     * {@inheritdoc}
     */
    public function toString(): string
    {
        return $this->name;
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

        $this->name = $channel;
    }
}
