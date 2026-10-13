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

namespace Fabpot\Amp\Sqlite\Internal;

use Amp\DeferredFuture;
use Fabpot\Amp\Sqlite\Worker\DTO\OperationResultDTO;
use Fabpot\Amp\Sqlite\Worker\Mode;

/** Result ownership and accounting; never retains the serialized request data. @internal */
final class PipelineOperation
{
    public PipelineOperationState $state = PipelineOperationState::Queued;
    public bool $dispatched = false;
    public bool $writerReleased = false;
    public bool $consumerFinished = false;
    public bool $flightReleased = false;
    public bool $admissionReleased = false;

    /** @param DeferredFuture<OperationResultDTO> $result */
    public function __construct(
        public readonly DeferredFuture $result,
        public readonly Mode $mode,
        public readonly int $maximumResultBytes,
        public readonly int $creditBytes,
    ) {
        $result->getFuture()->ignore();
    }
}
