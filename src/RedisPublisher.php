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
use Amp\Redis\Client;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use ServiceBus\Transport\Common\Package\OutboundPackage;
use ServiceBus\Transport\Common\Transport;

/**
 *
 */
final class RedisPublisher
{
    /**
     * @var Client|null
     */
    private $publishClient;

    /**
     * @var RedisTransportConnectionConfiguration
     */
    private $config;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @param RedisTransportConnectionConfiguration $config
     * @param LoggerInterface|null                  $logger
     */
    public function __construct(RedisTransportConnectionConfiguration $config, ?LoggerInterface $logger = null)
    {
        $this->config = $config;
        $this->logger = $logger ?? new NullLogger();
    }

    /**
     * Close connection.
     *
     * @return void
     */
    public function disconnect(): void
    {
        if (null !== $this->publishClient)
        {
            $this->publishClient->close();
        }
    }

    /**
     * Send message to Redis server.
     *
     * @param OutboundPackage $outboundPackage
     *
     * @return Promise
     */
    public function publish(OutboundPackage $outboundPackage): Promise
    {
        /**
         * @psalm-suppress MixedTypeCoercion
         * @psalm-suppress InvalidArgument
         */
        return call(
            function(OutboundPackage $outboundPackage): \Generator
            {
                if (null === $this->publishClient)
                {
                    $this->publishClient = new Client((string) $this->config);
                }

                $internalHeaders = [Transport::SERVICE_BUS_TRACE_HEADER => $outboundPackage->traceId];

                /** @var RedisTransportLevelDestination $destination */
                $destination        = $outboundPackage->destination;
                $destinationChannel = (string) $destination->channel;
                $headers            = \array_filter(\array_merge($internalHeaders, $outboundPackage->headers));

                /** @var string $package */
                $package = \json_encode(
                    [$outboundPackage->payload, $headers],
                    \JSON_UNESCAPED_UNICODE | \JSON_UNESCAPED_SLASHES
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
