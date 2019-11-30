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
     */
    private string $id;

    private string $fromChannel;

    private string $payload;

    /**
     * @psalm-var array<string, string|int|float>
     */
    private array

 $headers;

    /**
     * @psalm-param array<string, string|int|float> $headers
     */
    public function __construct(string $payload, array $headers, string $fromChannel)
    {
        $this->id          = uuid();
        $this->payload     = $payload;
        $this->headers     = $headers;
        $this->fromChannel = $fromChannel;
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
}
