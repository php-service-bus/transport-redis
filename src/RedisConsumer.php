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

use function Amp\asyncCall;
use function Amp\call;
use Amp\Promise;
use Amp\Redis\SubscribeClient;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use ServiceBus\Transport\Common\Exceptions\ConnectionFail;

/**
 * @internal
 */
final class RedisConsumer
{
    /**
     * @var RedisChannel
     */
    private $channel;

    /**
     * @var RedisTransportConnectionConfiguration
     */
    private $config;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var SubscribeClient|null
     */
    private $subscribeClient;

    /**
     * @param RedisChannel                          $channel
     * @param RedisTransportConnectionConfiguration $config
     * @param LoggerInterface|null                  $logger
     */
    public function __construct(
        RedisChannel $channel,
        RedisTransportConnectionConfiguration $config,
        ?LoggerInterface $logger = null
    ) {
        $this->channel = $channel;
        $this->config  = $config;
        $this->logger  = $logger ?? new NullLogger();
    }

    /**
     * Listen channel messages.
     *
     * @param callable $onMessage
     *
     * @throws \ServiceBus\Transport\Common\Exceptions\ConnectionFail Connection refused
     *
     * @return Promise
     */
    public function listen(callable $onMessage): Promise
    {
        /** @psalm-suppress MixedTypeCoercion */
        return call(
            function() use ($onMessage): \Generator
            {
                $this->subscribeClient = new SubscribeClient((string) $this->config);

                $channelName = (string) $this->channel;

                $this->logger->info('Creates new consumer for channel "{channelName}" ', [
                    'channelName' => $channelName,
                ]);

                try
                {
                    /**
                     * @psalm-suppress TooManyTemplateParams
                     *
                     * @var \Amp\Redis\Subscription $subscription
                     */
                    $subscription = yield $this->subscribeClient->subscribe($channelName);
                }
                catch (\Throwable $throwable)
                {
                    throw ConnectionFail::fromThrowable($throwable);
                }

                /** @psalm-suppress TooManyTemplateParams */
                while (yield $subscription->advance())
                {
                    try
                    {
                        /** @var string $jsonMessage */
                        $jsonMessage = $subscription->getCurrent();

                        self::handleMessage($jsonMessage, $channelName, $onMessage);
                    }
                    // @codeCoverageIgnoreStart
                    catch (\Throwable $throwable)
                    {
                        $this->logger->error('Emit package failed: {throwableMessage} ', [
                            'throwableMessage' => $throwable->getMessage(),
                            'throwablePoint'   => \sprintf('%s:%d', $throwable->getFile(), $throwable->getLine()),
                        ]);
                    }
                    // @codeCoverageIgnoreEnd
                }
            }
        );
    }

    /**
     * Call message handler.
     *
     * @param string   $messagePayload
     * @param string   $onChannel
     * @param callable $onMessage
     *
     * @return void
     */
    private static function handleMessage(string $messagePayload, string $onChannel, callable $onMessage): void
    {
        /** @var array|false $decoded */
        $decoded = \json_decode($messagePayload, true);

        /** @psalm-suppress RedundantConditionGivenDocblockType */
        if (
            false !== $decoded && \JSON_ERROR_NONE === \json_last_error() &&
            true === \is_array($decoded) && 2 === \count($decoded)
        ) {
            /**
             * @psalm-var string $body
             * @psalm-var array<string, string|int|float> $headers
             */
            [$body, $headers] = $decoded;

            /** @psalm-suppress InvalidArgument */
            asyncCall($onMessage, RedisIncomingPackage::create($body, $headers, $onChannel));

            return;
        }

        /**
         * Message without headers.
         *
         * @psalm-suppress InvalidArgument
         */
        asyncCall($onMessage, RedisIncomingPackage::create($messagePayload, [], $onChannel));
    }

    /**
     * Stop watching the channel.
     *
     * @return Promise It does not return any result
     */
    public function stop(): Promise
    {
        /** @psalm-suppress MixedTypeCoercion */
        return call(
            function(): void
            {
                if (null === $this->subscribeClient)
                {
                    return;
                }

                $this->subscribeClient->close();

                $this->logger->info('Subscription canceled', ['channelName' => (string) $this->channel]);
            }
        );
    }
}
