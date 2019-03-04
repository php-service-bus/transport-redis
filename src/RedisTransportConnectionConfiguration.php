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

use ServiceBus\Transport\Redis\Exceptions\IncorrectConnectionParameters;

/**
 * Connection parameters.
 *
 * @property-read string      $scheme
 * @property-read string      $host
 * @property-read int         $port
 * @property-read int         $timeout
 * @property-read string|null $password
 */
final class RedisTransportConnectionConfiguration
{
    private const DEFAULT_HOST    = 'localhost';

    private const DEFAULT_PORT    = 6379;

    private const DEFAULT_TIMEOUT = 5;

    /**
     * @var string
     */
    public $scheme;

    /**
     * @var string
     */
    public $host;

    /**
     * @var int
     */
    public $port;

    /**
     * @var int
     */
    public $timeout;

    /**
     * @var string|null
     */
    public $password;

    /**
     * @param string $connectionDSN
     *
     * @throws \ServiceBus\Transport\Redis\Exceptions\IncorrectConnectionParameters
     */
    public function __construct(string $connectionDSN)
    {
        $parameters = self::parseUrl($connectionDSN);

        $queryString = (string) ($parameters['query'] ?? '');

        $query = self::parseQuery($queryString);

        $this->scheme   = (string) $parameters['scheme'];
        $this->host     = isset($parameters['host']) ? (string) $parameters['host'] : self::DEFAULT_HOST;
        $this->port     = isset($parameters['port']) ? (int) $parameters['port'] : self::DEFAULT_PORT;
        $this->password = isset($query['password']) ? (string) $query['password'] : null;
        $this->timeout  = isset($query['timeout']) ? (int) $query['timeout'] : self::DEFAULT_TIMEOUT;

        if (0 > $this->timeout)
        {
            $this->timeout = self::DEFAULT_TIMEOUT;
        }
    }

    /**
     * @return string
     */
    public function __toString()
    {
        return \sprintf(
            '%s://%s:%s?%s',
            $this->scheme,
            $this->host,
            $this->port,
            \http_build_query(\array_filter(['timeout' => $this->timeout * 1000, 'password' => $this->password]))
        );
    }

    /**
     * @param string $connectionDSN
     *
     * @throws \ServiceBus\Transport\Redis\Exceptions\IncorrectConnectionParameters
     *
     * @return array
     *
     */
    private static function parseUrl(string $connectionDSN): array
    {
        if ('' === $connectionDSN)
        {
            throw IncorrectConnectionParameters::connectionDsnCantBeEmpty();
        }

        if (0 !== \strpos($connectionDSN, 'tcp://') && 0 !== \strpos($connectionDSN, 'unix://'))
        {
            throw IncorrectConnectionParameters::incorrectScheme();
        }

        $parsedParts = \parse_url($connectionDSN);

        if (false !== $parsedParts)
        {
            return $parsedParts;
        }

        throw IncorrectConnectionParameters::incorrectDSN($connectionDSN);
    }

    /**
     * @psalm-return array<string, string|int|float>
     *
     * @param string $connectionDSN
     *
     * @return array
     */
    private static function parseQuery(string $connectionDSN): array
    {
        $output = [];

        \parse_str($connectionDSN, $output);

        /** @psalm-var array<string, string|int|float> $output */

        return $output;
    }
}
