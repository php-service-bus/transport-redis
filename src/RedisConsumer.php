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
use Amp\Redis\Config;
use Amp\Redis\Subscriber;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use ServiceBus\Transport\Common\Exceptions\ConnectionFail;
use function ServiceBus\Common\jsonDecode;

/**
 * @internal
 */
final class RedisConsumer
{
    private RedisChannel $channel;

    private RedisTransportConnectionConfiguration $config;

    private LoggerInterface $logger;

    private ?Subscriber $subscribeClient = null;

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
     * @throws \ServiceBus\Transport\Common\Exceptions\ConnectionFail Connection refused
     */
    public function listen(callable $onMessage): Promise
    {
        return call(
            function () use ($onMessage): \Generator
            {
                if ($this->subscribeClient === null)
                {
                    $this->subscribeClient = new Subscriber(Config::fromUri($this->config->toString()));
                }

                $this->logger->info('Creates new consumer for channel "{channelName}" ', [
                    'channelName' => $this->channel->name,
                ]);

                try
                {
                    /**
                     * @psalm-suppress TooManyTemplateParams
                     *
                     * @var \Amp\Redis\Subscription $subscription
                     */
                    $subscription = yield $this->subscribeClient->subscribe($this->channel->toString());
                }
                catch (\Throwable $throwable)
                {
                    throw ConnectionFail::fromThrowable($throwable);
                }

                while (yield $subscription->advance())
                {
                    try
                    {
                        /** @var string $jsonMessage */
                        $jsonMessage = $subscription->getCurrent();

                        self::handleMessage($jsonMessage, $this->channel->name, $onMessage);
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
     * @throws \Throwable json decode failed
     */
    private static function handleMessage(string $messagePayload, string $onChannel, callable $onMessage): void
    {
        $decoded = jsonDecode($messagePayload);

        if (\count($decoded) === 2)
        {
            /**
             * @psalm-var string $body
             * @psalm-var array<string, string|int|float> $headers
             */
            [$body, $headers] = $decoded;

            /** @psalm-suppress InvalidArgument */
            asyncCall($onMessage, new RedisIncomingPackage($body, $headers, $onChannel));

            return;
        }

        /**
         * Message without headers.
         *
         * @psalm-suppress InvalidArgument
         */
        asyncCall($onMessage, new RedisIncomingPackage($messagePayload, [], $onChannel));
    }

    /**
     * Stop watching the channel.
     *
     * @return Promise It does not return any result
     */
    public function stop(): Promise
    {
        return call(
            function (): void
            {
                if ($this->subscribeClient === null)
                {
                    return;
                }

                $this->subscribeClient = null;

                $this->logger->info('Subscription canceled', ['channelName' => $this->channel->name]);
            }
        );
    }
}
