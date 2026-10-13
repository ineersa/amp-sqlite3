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

/** Request data belongs only to the FIFO and then to the writer's stack. @internal */
final readonly class QueuedPipelineOperation
{
    /** @param array<string, mixed> $request */
    public function __construct(public PipelineOperation $operation, public array $request)
    {
    }
}
