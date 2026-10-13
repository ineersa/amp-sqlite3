<?php

declare(strict_types=1);

/*
 * This file is part of the fabpot/amphp-sqlite3 package.
 *
 * (c) Fabien Potencier <fabien@potencier.org>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Fabpot\Amp\Sqlite\Worker\DTO;

/**
 * Immutable generic limits; byte values include serialized metadata, not just BLOB bodies.
 * maximumErrorReplyBytes independently caps the complete serialized error envelope, including SQL and native codes.
 * An oversized error fails the connection. Neither SQL nor native diagnostics are truncated to fit.
 */
final readonly class PipelineLimitsDTO
{
    /** Conservative reserve for Amp framing and the response's id/value wrapper. */
    public const CHANNEL_OVERHEAD_BYTES = 512;
    public const DEFAULT_ERROR_REPLY_BYTES = 8192;
    private const DEFAULT_ADMITTED_OPERATIONS = 128;
    private const DEFAULT_ADMITTED_WINDOWS = 64;

    /** Depth one uses the identical coordinator; callers may configure a larger dispatch window. */
    public static function serial(int $maximumResultBytes): self
    {
        if ($maximumResultBytes < 1) {
            throw new \InvalidArgumentException('Result byte budget must be positive');
        }
        $flight = 2 * ($maximumResultBytes + self::DEFAULT_ERROR_REPLY_BYTES + self::CHANNEL_OVERHEAD_BYTES);

        return new self(1, $flight, self::DEFAULT_ADMITTED_OPERATIONS, self::DEFAULT_ADMITTED_WINDOWS * $flight, self::DEFAULT_ERROR_REPLY_BYTES);
    }

    public function __construct(
        public int $maximumInFlightOperations,
        public int $maximumInFlightBytes,
        public int $maximumAdmittedOperations,
        public int $maximumAdmittedBytes,
        public int $maximumErrorReplyBytes,
    ) {
        if ($maximumInFlightOperations < 1) {
            throw new \InvalidArgumentException('Pipeline depth must be positive');
        }
        if ($maximumInFlightBytes < 1) {
            throw new \InvalidArgumentException('In-flight byte limit must be positive');
        }
        if ($maximumAdmittedOperations < $maximumInFlightOperations) {
            throw new \InvalidArgumentException('Admission count must cover the pipeline depth');
        }
        if ($maximumAdmittedBytes < $maximumInFlightBytes) {
            throw new \InvalidArgumentException('Admission bytes must cover the in-flight limit');
        }
        if ($maximumErrorReplyBytes < self::CHANNEL_OVERHEAD_BYTES) {
            throw new \InvalidArgumentException('Error reply budget must cover channel overhead');
        }
        if ($maximumErrorReplyBytes > $maximumInFlightBytes) {
            throw new \InvalidArgumentException('Error reply budget must fit the in-flight limit');
        }
    }
}
