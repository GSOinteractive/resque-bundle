<?php

namespace AllProgrammic\Bundle\ResqueBundle\Service\Monitoring;


use AllProgrammic\Bundle\ResqueBundle\Service\MailSender;
use AllProgrammic\Component\Resque\AbstractJob;
use AllProgrammic\Component\Resque\Engine;

class QueueThresholdAlertJob extends AbstractJob
{
    /**
     * @var Engine
     */
    private $engine;
    /**
     * @var MailSender
     */
    private $mailSender;

    public function __construct(Engine $engine, MailSender $mailSender)
    {
        $this->engine = $engine;
        $this->mailSender = $mailSender;
    }

    public function perform($args)
    {
        try {
            $options = $this->normalizeOptions($args);
        } catch (\Exception $e) {
            throw new \RuntimeException("an invalid argument was passed to job", 0, $e);
        }

        foreach ($this->engine->queues() as $queue) {
            if (preg_match($options['queues'], $queue) && !preg_match($options['excludedQueues'], $queue)) {
                $alertStatus = $this->engine->getBackend()->get(static::class.':'.$queue);
                $size = $this->engine->size($queue);
                if (!$alertStatus && $size > $options['threshold']) {
                    $this->engine->getBackend()->set(static::class.':'.$queue, (new \DateTimeImmutable())->format('c'));
                    $this->mailSender->send('monitoring/queue-threshold-alert/above', [
                        'queue' => $queue,
                        'threshold' => $options['threshold'],
                        'size' => $size,
                    ]);
                } else if ($alertStatus && $size <= $options['threshold']) {
                    $this->engine->getBackend()->del(static::class.':'.$queue);
                    $this->mailSender->send('monitoring/queue-threshold-alert/below', [
                        'queue' => $queue,
                        'threshold' => $options['threshold'],
                        'size' => $size,
                    ]);
                }
            }
        }
    }

    /**
     * @throws \Exception
     */
    protected function normalizeOptions($args)
    {
        $options = [
            'queues' => '/.*/',
            'excludedQueues' => null,
            'threshold' => null,
        ];

        if (is_array($args)) {
            if (isset($args['queues'])) {
                $options['queues'] = $args['queues'];
            }
            if (isset($args['excludedQueues'])) {
                $options['excludedQueues'] = $args['excludedQueues'];
            }
            if (isset($args['threshold'])) {
                $options['threshold'] = filter_var($args['threshold'], FILTER_VALIDATE_INT, ['options' => ['min_range' => 1, 'default' => null]]);
            }
        }

        if (@preg_match($options['queues'], 'fake') === false) {
            throw new \InvalidArgumentException(sprintf('Provided argument "queues: %s" must be a valid regex pattern with delimiter.', isset($args['queues']) ? $args['queues'] : null));
        }

        if (@preg_match($options['excludedQueues'], 'fake') === false) {
            throw new \InvalidArgumentException(sprintf('Provided argument "excludedQueues: %s" must be a valid regex pattern with delimiter.', isset($args['excludedQueues']) ? $args['excludedQueues'] : null));
        }

        if (empty($options['threshold'])) {
            throw new \InvalidArgumentException(sprintf('Provided argument "threshold: %s" must be defined as a positive integer.', isset($args['threshold']) ? $args['threshold'] : null));
        }

        return $options;
    }
}