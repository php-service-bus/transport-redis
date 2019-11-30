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

use function Amp\call;
use Amp\Promise;
use Amp\Redis\Config;
use Amp\Redis\Redis;
use Amp\Redis\RemoteExecutor;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use ServiceBus\Transport\Common\Package\OutboundPackage;
use ServiceBus\Transport\Common\Transport;

/**
 *
 */
final class RedisPublisher
{
    private ?Redis $publishClient;

    private RedisTransportConnectionConfiguration $config;

    private LoggerInterface $logger;

    public function __construct(RedisTransportConnectionConfiguration $config, ?LoggerInterface $logger = null)
    {
        $this->config = $config;
        $this->logger = $logger ?? new NullLogger();
    }

    /**
     * Close connection.
     */
    public function disconnect(): void
    {
        if (true === isset($this->publishClient))
        {
            $this->publishClient = null;
        }
    }

    /**
     * Send message to Redis server.
     */
    public function publish(OutboundPackage $outboundPackage): Promise
    {
        return call(
            function(OutboundPackage $outboundPackage): \Generator
            {
                if (false === isset($this->publishClient))
                {
                    $this->publishClient = new Redis(
                        new RemoteExecutor(Config::fromUri($this->config->toString()))
                    );
                }

                $internalHeaders = [Transport::SERVICE_BUS_TRACE_HEADER => $outboundPackage->traceId];

                /** @var RedisTransportLevelDestination $destination */
                $destination        = $outboundPackage->destination;
                $destinationChannel = $destination->channel;
                $headers            = \array_filter(\array_merge($internalHeaders, $outboundPackage->headers));

                /** @var string $package */
                $package = \json_encode(
                    [$outboundPackage->payload, $headers],
                    \JSON_UNESCAPED_UNICODE | \JSON_UNESCAPED_SLASHES | \JSON_THROW_ON_ERROR
                );

                $this->logger->debug('Publish message to "{channelName}"', [
                    'traceId'     => $outboundPackage->traceId,
                    'channelName' => $destinationChannel,
                    'content'     => $package,
                    'isMandatory' => $outboundPackage->mandatoryFlag,
                ]);

                /** @var int $result */
                $result = yield $this->publishClient->publish($destinationChannel, $package);

                if (0 === $result && true === $outboundPackage->mandatoryFlag)
                {
                    $this->logger->critical('Publish message failed', [
                        'channelName' => $destinationChannel,
                        'content'     => $package,
                    ]);
                }
            },
            $outboundPackage
        );
    }
}
