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

use function ServiceBus\Common\uuid;
use Amp\Promise;
use Amp\Success;
use ServiceBus\Transport\Common\DeliveryDestination;
use ServiceBus\Transport\Common\Package\IncomingPackage;
use ServiceBus\Transport\Common\Transport;

/**
 *
 */
final class RedisIncomingPackage implements IncomingPackage
{
    /**
     * Received package id.
     *
     * @var string
     */
    private $id;

    /**
     * @var string
     */
    private $fromChannel;

    /**
     * The time the message was received (Unix timestamp with microseconds).
     *
     * @var float
     */
    private $time;

    /**
     * @var string
     */
    private $payload;

    /**
     * @psalm-var array<string, string|int|float>
     *
     * @var array
     */
    private $headers = [];

    /**
     * @psalm-param array<string, string|int|float> $headers
     *
     * @param string $payload
     * @param array  $headers
     * @param string $fromChannel
     *
     * @return self
     */
    public static function create(string $payload, array $headers, string $fromChannel): self
    {
        $self = new self();

        $self->payload     = $payload;
        $self->headers     = $headers;
        $self->fromChannel = $fromChannel;

        return $self;
    }

    /**
     * {@inheritdoc}
     */
    public function id(): string
    {
        return $this->id;
    }

    /**
     * {@inheritdoc}
     */
    public function time(): float
    {
        return $this->time;
    }

    /**
     * {@inheritdoc}
     */
    public function origin(): DeliveryDestination
    {
        return new RedisTransportLevelDestination($this->fromChannel);
    }

    /**
     * {@inheritdoc}
     */
    public function payload(): string
    {
        return $this->payload;
    }

    /**
     * {@inheritdoc}
     */
    public function headers(): array
    {
        return $this->headers;
    }

    /**
     * @codeCoverageIgnore
     *
     * {@inheritdoc}
     */
    public function ack(): Promise
    {
        return new Success();
    }

    /**
     * @codeCoverageIgnore
     *
     * {@inheritdoc}
     */
    public function nack(bool $requeue, ?string $withReason = null): Promise
    {
        return new Success();
    }

    /**
     * @codeCoverageIgnore
     *
     * {@inheritdoc}
     */
    public function reject(bool $requeue, ?string $withReason = null): Promise
    {
        return new Success();
    }

    /**
     * {@inheritdoc}
     */
    public function traceId()
    {
        $traceId = (string) ($this->headers[Transport::SERVICE_BUS_TRACE_HEADER] ?? '');

        if ('' === $traceId)
        {
            $traceId = uuid();
        }

        return $traceId;
    }

    /**
     */
    private function __construct()
    {
        $this->id   = uuid();
        $this->time = (float) \microtime(true);
    }
}
