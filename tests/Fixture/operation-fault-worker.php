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

namespace Fabpot\Amp\Sqlite\Test\Fixture;

use Amp\ByteStream\StreamChannel;
use Amp\Sync\Channel;
use function Amp\Socket\connect;

enum OperationFaultCommand: string
{
    case Continue = 'continue';
    case ExitBeforeCommit = 'exit_before_commit';
    case LoseReply = 'lose_reply';
    case MalformedReply = 'malformed_reply';
}

final class OperationFaultBarrier
{
    private bool $malformed = false;

    public function __construct(private readonly Channel $control)
    {
    }

    public function at(string $stage): void
    {
        $this->control->send($stage);
        $command = $this->control->receive();
        if (!\is_string($command)) {
            throw new \RuntimeException('Worker fault command must be a string');
        }
        match (OperationFaultCommand::from($command)) {
            OperationFaultCommand::Continue => null,
            OperationFaultCommand::ExitBeforeCommit => exit(1),
            OperationFaultCommand::LoseReply => exit(0),
            OperationFaultCommand::MalformedReply => $this->malformed = true,
        };
    }

    /** @param array<string, mixed> $value
     * @return array<string, mixed>
     */
    public function reply(array $value): array
    {
        return $this->malformed ? ['outcome' => 'commit', 'data' => 'invalid'] : $value;
    }
}

// The test copies contain exact-anchor hooks; production source contains none.
require $argv[1];
$socket = connect($argv[3]);
$GLOBALS['operationFaultBarrier'] = new OperationFaultBarrier(new StreamChannel($socket, $socket));

return require $argv[2];
