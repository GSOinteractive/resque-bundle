<?php

namespace AllProgrammic\Bundle\ResqueBundle\Service\Monitoring;


use AllProgrammic\Component\Resque\AbstractJob;
use AllProgrammic\Component\Resque\Engine;

class QueueThresholdAlertJob extends AbstractJob
{
    /**
     * @var Engine
     */
    protected $engine;
    /**
     * @var string
     */
    protected $env;

    public function __construct(Engine $engine, $env)
    {
        $this->engine = $engine;
        $this->env = $env;
    }

    public function perform($args)
    {
        try {
            $options = $this->normalizeOptions($args);
        } catch (\Exception $e) {
            throw new \RuntimeException("an invalid argument was passed to job", 0, $e);
        }

        foreach ($this->engine->queues() as $queue) {
            if (preg_match($options['queues'], $queue) && (empty($options['excludedQueues']) || !preg_match($options['excludedQueues'], $queue))) {
                $now = new \DateTimeImmutable();
                $size = $this->engine->size($queue);

                $alertStatus = $this->parseAlertStatus($this->engine->getBackend()->get('monitoring:'.static::class.':'.$queue));
                $alert = $alertStatus['sendingNumber'] > 0;

                // s'il n'y avait pas encore d'alerte et que la taille dépasse le seuil
                // ou qu'il y avait déjà une alert et qu'il faut la relancer en fonction de l'option snooze
                $snooze = (!empty($options['snoozeMinuteDelay']) && $alert
                    && $alertStatus['since']->add(new \DateInterval('PT'.($alertStatus['sendingNumber'] * $options['snoozeMinuteDelay']).'M')) <= $now);
                if ((!$alert || $snooze) && $size > $options['threshold']) {
                    $alertStatus['sendingNumber']++;
                    $this->engine->getBackend()->set('monitoring:'.static::class.':'.$queue, $this->serializeAlertStatus($alertStatus));
                    $this->raiseAlert($alertStatus, $queue, $options['threshold'], $size);
                    // sinon s'il y avait déjà une alerte et que la taille est en dessous du seuil
                } else if ($alert && $size <= $options['threshold']) {
                    $this->engine->getBackend()->del('monitoring:'.static::class.':'.$queue);
                    // si on veut des alerts lors du retour à la normale
                    if ($options['alertOnBackToNormal']) {
                        $this->backToNormal($alertStatus, $queue, $options['threshold'], $size);
                    }
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
            'snoozeMinuteDelay' => null,
            'alertOnBackToNormal' => true,
        ];

        if (is_array($args)) {
            if (isset($args['queues'])) {
                $options['queues'] = $args['queues'];
            }
            if (isset($args['excludedQueues'])) {
                $options['excludedQueues'] = $args['excludedQueues'];
            }
            if (isset($args['threshold']) && !empty($args['threshold'])) {
                $options['threshold'] = filter_var($args['threshold'], FILTER_VALIDATE_INT, ['options' => ['min_range' => 1, 'default' => null]]);
            }
            if (isset($args['snoozeMinuteDelay']) && !empty($args['snoozeMinuteDelay'])) {
                $options['snoozeMinuteDelay'] = filter_var($args['snoozeMinuteDelay'], FILTER_VALIDATE_INT, ['options' => ['min_range' => 1, 'default' => null]]);
            }
            if (isset($args['alertOnBackToNormal']) && !empty($args['alertOnBackToNormal'])) {
                $options['alertOnBackToNormal'] = filter_var($args['alertOnBackToNormal'], FILTER_VALIDATE_BOOLEAN, ['options' => ['default' => true]]);
            }
        }

        if (@preg_match($options['queues'], 'fake') === false) {
            throw new \InvalidArgumentException(sprintf('Provided argument "queues: %s" must be a valid regex pattern with delimiter.', isset($args['queues']) ? $args['queues'] : null));
        }

        if (!empty($options['excludedQueues']) && @preg_match($options['excludedQueues'], 'fake') === false) {
            throw new \InvalidArgumentException(sprintf('Provided argument "excludedQueues: %s" must be a valid regex pattern with delimiter.', isset($args['excludedQueues']) ? $args['excludedQueues'] : null));
        }

        if (empty($options['threshold'])) {
            throw new \InvalidArgumentException(sprintf('Provided argument "threshold: %s" must be defined as a positive integer.', isset($args['threshold']) ? $args['threshold'] : null));
        }

        return $options;
    }

    /**
     * @param string $alertStatus
     * @return array
     */
    protected function parseAlertStatus($alertStatus)
    {
        $alertStatus = json_decode($alertStatus, true);
        // cas d'une valeur corrompue, dans ce cas, on considère qu'elle n'existe pas
        if ($alertStatus === false || !isset($alertStatus['since']) || !isset($alertStatus['sendingNumber'])) {
            $alertStatus = [
                'since' => new \DateTimeImmutable(),
                'sendingNumber' => 0,
            ];
        } else {
            try {
                $alertStatus['since'] = new \DateTimeImmutable($alertStatus['since']);
            } catch (\Exception $e) {
                $alertStatus['since'] = new \DateTimeImmutable();
            }
        }

        return $alertStatus;
    }

    /**
     * @param array $alertStatus
     * @return false|string
     */
    private function serializeAlertStatus(array $alertStatus)
    {
        $alertStatus['since'] = $alertStatus['since']->format('c');
        return json_encode($alertStatus);
    }

    /**
     * @param array{since: \DateTimeInterface, sendingNumber: int} $alertStatus
     * @param string $queue
     * @param int $threshold
     * @param int $size
     * @return void
     */
    protected function raiseAlert(array $alertStatus, $queue, $threshold, $size)
    {
        $this->engine->sendMail('monitoring/queue-threshold-alert/above', [
            'subject' => ($alertStatus['sendingNumber'] == 1
                ? sprintf('[Resque Monitoring][%s] %s size is above %d', $this->env, $queue, $threshold)
                : sprintf('[Resque Monitoring][%s] REMINDER #%d - %s size is above %d', $this->env, $alertStatus['sendingNumber'], $queue, $threshold)),
            'queue' => $queue,
            'threshold' => $threshold,
            'size' => $size,
            'env' => $this->env,
            'alertStatus' => $alertStatus,
        ]);
    }

    /**
     * @param array{since: \DateTimeInterface, sendingNumber: int} $alertStatus
     * @param string $queue
     * @param int $threshold
     * @param int $size
     * @return void
     */
    protected function backToNormal(array $alertStatus, $queue, $threshold, $size)
    {
        $this->engine->sendMail('monitoring/queue-threshold-alert/below', [
            'subject' => sprintf('[Resque Monitoring][%s] %s size is back below %d', $this->env, $queue, $threshold),
            'queue' => $queue,
            'threshold' => $threshold,
            'size' => $size,
            'env' => $this->env,
            'alertStatus' => $alertStatus,
        ]);
    }
}