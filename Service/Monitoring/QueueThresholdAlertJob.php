<?php

namespace AllProgrammic\Bundle\ResqueBundle\Service\Monitoring;


use AllProgrammic\Component\Resque\AbstractJob;
use AllProgrammic\Component\Resque\Engine;

class QueueThresholdAlertJob extends AbstractJob
{
    /**
     * @var Engine
     */
    private $engine;

    public function __construct(Engine $engine)
    {
        $this->engine = $engine;
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
                    $this->engine->sendMail('monitoring/queue-threshold-alert/above', [
                        'subject' => ($alertStatus['sendingNumber'] == 1
                            ? sprintf('[Resque Monitoring] %s size is above %d', $queue, $options['threshold'])
                            : sprintf('[Resque Monitoring] REMINDER #%d - %s size is above %d', $alertStatus['sendingNumber'], $queue, $options['threshold'])),
                        'queue' => $queue,
                        'threshold' => $options['threshold'],
                        'size' => $size,
                        'alertStatus' => $alertStatus,
                    ]);
                // sinon si on veut des alerts lors du retour à la normal et qu'il y avait déjà une alert et que la taille est en dessous du seuil
                } else if ($options['alertOnBackToNormal'] && $alert && $size <= $options['threshold']) {
                    $this->engine->getBackend()->del('monitoring:'.static::class.':'.$queue);
                    $this->engine->sendMail('monitoring/queue-threshold-alert/below', [
                        'subject' => sprintf('[Resque Monitoring] %s size is back below %d', $queue, $options['threshold']),
                        'queue' => $queue,
                        'threshold' => $options['threshold'],
                        'size' => $size,
                        'alertStatus' => $alertStatus,
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
}