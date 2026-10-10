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

enum InsertFaultCommand: string
{
    case Continue = 'continue';
    case Die = 'die';
    case Close = 'close';
    case Malformed = 'malformed';
}

final class InsertFaultBarrier
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
            throw new \RuntimeException('Fault command must be a string');
        }
        $command = InsertFaultCommand::from($command);
        match ($command) {
            InsertFaultCommand::Continue => null,
            InsertFaultCommand::Die => exit(1),
            InsertFaultCommand::Close => exit(0),
            InsertFaultCommand::Malformed => $this->malformed = true,
        };
    }

    public function reply(int $id): mixed
    {
        return $this->malformed ? ['invalid_id' => $id] : $id;
    }
}

// Arguments are supplied by the real ProcessContextFactory. Only generated
// test-local source copies contain hooks; installed production files stay intact.
require $argv[1];
$socket = connect($argv[3]);
$GLOBALS['insertFaultBarrier'] = new InsertFaultBarrier(new StreamChannel($socket, $socket));

return require $argv[2];
