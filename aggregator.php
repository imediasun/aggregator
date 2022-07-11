<?php

class Aggregator {

    private $conf = [

    ];

    public $state = [
        // Specifies the grouping column, otherwise counts across all rows
        'group_key' => null,

        // If a grouping column is specified, there will be a nested array with each group's key values.
        // Else just a flat array of common values
        'group_data' => [],

        'every' => [],
        'everyCounter' => 0,

        'formula' => [],
    ];

    /**
     * Describe the structure of each group
     */
    public function sum($keyIn, $keyOut) {
        $this->conf[] = [
            'method' => 'sum',
            'keyIn'  => $keyIn,
            'keyOut' => $keyOut,
        ];

        return $this;
    }

    public function avg($keyIn, $keyOut) {
        $this->conf[] = [
            'method' => 'avg',
            'keyIn'  => $keyIn,
            'keyOut' => $keyOut,
        ];

        return $this;
    }

    public function min($keyIn, $keyOut) {
        $this->conf[] = [
            'method' => 'min',
            'keyIn'  => $keyIn,
            'keyOut' => $keyOut,
        ];

        return $this;
    }

    public function max($keyIn, $keyOut) {
        $this->conf[] = [
            'method' => 'max',
            'keyIn'  => $keyIn,
            'keyOut' => $keyOut,
        ];

        return $this;
    }

    public function first($keyIn, $keyOut) {
        $this->conf[] = [
            'method' => 'first',
            'keyIn'  => $keyIn,
            'keyOut' => $keyOut,
        ];

        return $this;
    }

    public function last($keyIn, $keyOut) {
        $this->conf[] = [
            'method' => 'last',
            'keyIn'  => $keyIn,
            'keyOut' => $keyOut,
        ];

        return $this;
    }

    public function distinct($keyIn) {
        $this->conf[] = [
            'method' => 'distinct',
            'keyIn'  => $keyIn,
        ];

        return $this;
    }

    public function countIf($filter, $keyOut) {
        $this->conf[] = [
            'method' => 'countIf',
            'callback' => $filter,
            'keyOut' => $keyOut,
        ];

        return $this;
    }

    public function count($keyOut) {
        $this->conf[] = [
            'method' => 'count',
            'keyOut' => $keyOut,
        ];

        return $this;
    }

    public function groupBy($keyIn) {
        $this->state['group_key'] = $keyIn;

        return $this;
    }

    public function every($n, $callback) {
        $this->state['every'][$n] = $callback;

        return $this;
    }

    public function percentile($percentage, $keyIn, $keyOut) {
        $this->conf[] = [
            'method' => 'percentile',
            'percentage' => $percentage,
            'keyIn'  => $keyIn,
            'keyOut' => $keyOut,
        ];

        return $this;
    }

    public function sample($limit, $keyIn, $keyOut) {
        $this->conf[] = [
            'method' => 'sample',
            'limit' => $limit,
            'keyIn'  => $keyIn,
            'keyOut' => $keyOut,
        ];

        return $this;
    }

    public function formula($callback, $keyOut) {
        $this->state['formula'][] = [
            'callback' => $callback,
            'keyOut' => $keyOut,
        ];

        return $this;
    }

    public function add($row) {
        $this->state['everyCounter']++;

        // Determine whether our aggregator groups data
        $isGroupBy = false;
        if ($this->state['group_key']) {
            $isGroupBy = true;

            // The data cannot be processed because no grouping key value
            if (!array_key_exists($this->state['group_key'], $row)) {
                return false;
            }
        }

        // Create the initial data value of the aggregator
        $groupData = [
            'sum' => [],
            'min' => [],
            'max' => [],
            'first' => [],
            'last' => [],
            'distinct' => [],
            'countIf' => [],
            'count' => [],
            'sample' => [],
            'totalCount' => 0,
        ];

        if ($isGroupBy) {
            // If there is a grouping = init / get data
            $groupKeyValue = $row[$this->state['group_key']];

            if (array_key_exists($groupKeyValue, $this->state['group_data'])) {
                $groupData = $this->state['group_data'][$groupKeyValue];
            }
        } else if (!empty($this->state['group_data'])) {
            // If there is no grouping and empty data = do init
            $groupData = $this->state['group_data'];
        }

        // We consider the total number of lines
        $groupData['totalCount']++;

        // We execute the logic for each individual aggregator
        foreach ($this->conf as $filter) {
            $keyIn = $filter['keyIn'] ?? null;
            $rowValue = $row[$keyIn] ?? null;

            // Init data for each column
            foreach ($row as $rowKey => $_) {
                if (!array_key_exists($rowKey, $groupData['sum'])) {
                    $groupData['sum'][$rowKey] = 0;
                }
                if (!array_key_exists($rowKey, $groupData['distinct'])) {
                    $groupData['distinct'][$rowKey] = [];
                }
            }

            switch ($filter['method']) {
                case 'sum':
                    if (!array_key_exists($keyIn, $groupData['sum'])) {
                        $groupData['sum'][$keyIn] = 0;
                    }

                    $groupData['sum'][$keyIn] += $rowValue;
                    break;
                case 'min':
                    if (!array_key_exists($keyIn, $groupData['min']) || $rowValue < $groupData['min'][$keyIn]) {
                        $groupData['min'][$keyIn] = $rowValue;
                    }
                    break;
                case 'max':
                    if (!array_key_exists($keyIn, $groupData['max']) || $rowValue > $groupData['max'][$keyIn]) {
                        $groupData['max'][$keyIn] = $rowValue;
                    }
                    break;
                case 'first':
                    if (!array_key_exists($keyIn, $groupData['first'])) {
                        $groupData['first'][$keyIn] = $rowValue;
                    }
                    break;
                case 'last':
                    $groupData['last'][$keyIn] = $rowValue;
                    break;
                case 'distinct':
                    if (is_null($rowValue)) break;
                    if (!in_array($rowValue, $groupData['distinct'][$keyIn])) {
                        $groupData['distinct'][$keyIn][] = $rowValue;
                    }
                    break;
                case 'countIf':
                    if (!array_key_exists($filter['keyOut'], $groupData['countIf'])) {
                        $groupData['countIf'][$filter['keyOut']] = 0;
                    }

                    if ($filter['callback']($row) === true) {
                        $groupData['countIf'][$filter['keyOut']]++;
                    }
                    break;
                case 'sample':
                    if (!array_key_exists($keyIn, $groupData['sample'])) {
                        $groupData['sample'][$keyIn] = [];
                    }

                    $groupData['sample'][$keyIn][] = $rowValue;
                    break;
            }
        }

        if ($isGroupBy) {
            $this->state['group_data'][$groupKeyValue] = $groupData;
        } else {
            $this->state['group_data'] = $groupData;
        }

        // Выполняем every
        foreach ($this->state['every'] as $n => $callback) {
            if ($this->state['everyCounter'] % $n === 0) {
                $callback();
            }
        }
    }

    /**
     * Calculates the result of a state
     */
    public function get() {
        $result = [];

        $isGroupBy = false;
        if ($this->state['group_key']) {
            $isGroupBy = true;
        }

        /**
         * Counts the result of a state (all rows / each group of grouping rows)
         */
        $processRow = function($conf, $groupData) {
            $resultRow = [];

            foreach ($conf as $filter) {
                $keyIn = $filter['keyIn'] ?? null;

                switch ($filter['method']) {
                    case 'sum':
                        $resultRow[$filter['keyOut']] = $groupData['sum'][$keyIn];
                        break;
                    case 'avg':
                        // This aggregator has no state, it is considered when outputting
                        $resultRow[$filter['keyOut']] = $groupData['sum'][$keyIn] / $groupData['totalCount'];
                        break;
                    case 'min':
                        $resultRow[$filter['keyOut']] = $groupData['min'][$keyIn];
                        break;
                    case 'max':
                        $resultRow[$filter['keyOut']] = $groupData['max'][$keyIn];
                        break;
                    case 'first':
                        $resultRow[$filter['keyOut']] = $groupData['first'][$keyIn];
                        break;
                    case 'last':
                        $resultRow[$filter['keyOut']] = $groupData['last'][$keyIn];
                        break;
                    case 'distinct':
                        $resultRow[$filter['keyIn']] = implode(',', $groupData['distinct'][$keyIn]);
                        break;
                    case 'countIf':
                        $resultRow[$filter['keyOut']] = $groupData['countIf'][$filter['keyOut']];
                        break;
                    case 'count':
                        $resultRow[$filter['keyOut']] = $groupData['totalCount'];
                        break;
                    case 'sample':
                        $set = $groupData['sample'][$filter['keyIn']];
                        shuffle($set);

                        $resultRow[$filter['keyOut']] = array_splice($set, 0, $filter['limit']);
                        break;
                    case 'percentile':
                        // This aggregator has no state, it is considered when outputting
                        $resultRow[$filter['keyOut']] = $groupData['sum'][$keyIn] * ($filter['percentage'] / 100);
                        break;
                }
            }

            return $resultRow;
        };

        if ($isGroupBy) {
            foreach ($this->state['group_data'] as $groupKey => $groupData) {
                $row = $processRow($this->conf, $groupData);

                // Calling transformers
                foreach ($this->state['formula'] as $formula) {
                    $row[$formula['keyOut']] = $formula['callback']($row);
                }

                $result[] = $row;
            }
        } else {
            $row = $processRow($this->conf, $this->state['group_data']);

            // Calling transformers
            foreach ($this->state['formula'] as $formula) {
                $row[$formula['keyOut']] = $formula['callback']($row);
            }
        }

        // Cleaning state
//		$this->state['group_data'] = [];
//		$this->state['everyCounter'] = 0;

        return $result;
    }
}

$agg = new Aggregator;

$agg
    ->sum('points', 'sum_points')
    ->avg('points', 'avg_points')
    ->sum('score', 'sum_score')
    ->avg('score', 'avg_score')
    ->distinct('lesson_id')
    ->countIf(function($row) {
        return $row['score'] >= .8;
    }, 'good_score')
    ->countIf(function($row) {
        return $row['points'] > 500;
    }, 'high_points')
    ->count('cnt')
    ->groupBy('con_id')

    ->min('score', 'min_score')
    ->max('score', 'max_score')
    ->first('lesson_id', 'first_lesson_id')
    ->last('lesson_id', 'last_lesson_id')
    ->sample(2, 'points', 'sample_points')
    ->percentile(75, 'score', 'score_75th_percentile')
    ->formula(function($row) {
        $count = $row['min_score'];
        return $count ? round($row['cnt'] / $count, 2) : 0.0;
    }, 'record_perc')

    ->every(5, function() use ($agg) {
        //Here we can stream output data to any storage or send sockets
        $data = $agg->get();
        //$sendToSocket($data);
    })
;

$agg->add([
    'con_id'    => 1,
    'points'    => 100,
    'score'     => .8,
    'lesson_id' => 'a',
]);
$agg->add([
    'con_id'    => 1,
    'points'    => 100,
    'score'     => .5,
    'lesson_id' => 'a',
]);
$agg->add([
    'con_id'    => 1,
    'points'    => 400,
    'score'     => .2,
    'lesson_id' => 'b',
]);
$agg->add([
    'con_id'    => 2,
    'points'    => 100,
    'score'     => .5,
    'lesson_id' => 'a',
]);
$agg->add([
    'con_id'    => 2,
    'points'    => 600,
    'score'     => .5,
]);

echo "<pre>";
var_dump($agg->get());
echo "</pre>";

//It takes 0.28 msec Enjoy :)
$msec = microtime();
echo "Time as msec:" . $msec;