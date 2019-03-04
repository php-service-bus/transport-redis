<?php

/**
 * Redis transport implementation.
 *
 * @author  Maksim Masiukevich <dev@async-php.com>
 * @license MIT
 * @license https://opensource.org/licenses/MIT
 */

declare(strict_types = 1);

namespace ServiceBus\Transport\Redis\Exceptions;

/**
 *
 */
final class IncorrectConnectionParameters extends \InvalidArgumentException
{
    /**
     * @return self
     */
    public static function connectionDsnCantBeEmpty(): self
    {
        return new self('Connection DSN can\'t be empty');
    }

    /**
     * @return self
     */
    public static function incorrectScheme(): self
    {
        return new self('Connection DSN must start with tcp:// or unix://');
    }

    /**
     * @param string $dsn
     *
     * @return self
     */
    public static function incorrectDSN(string $dsn): self
    {
        return new self(\sprintf('Can\'t parse specified connection DSN (%s)', $dsn));
    }
}
