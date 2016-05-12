'use strict';

var util     = require('util'),
    protocol = require('./protocol'),
    debug    = require('debug')('kafka-node:common')
;


/**
 * Common functions that help keeping code DRY.
 */

/**
 * Validates options object passed to consumer constructors.
 *
 * TODO: Validate more!
 */
// var validateConsumerOptions = function(options) {
function validateConsumerOptions(options) {
    if ('autoOffsetReset' in options &&
        options.autoOffsetReset != protocol.OFFSET_LARGEST &&
        options.autoOffsetReset != protocol.OFFSET_SMALLEST) {
        throw new Error(
            util.format(
                "Failed validating options: autoOffsetReset %d is invalid. Must be either %d or %d",
                options.autoOffsetReset, protocol.OFFSET_LARGEST, protocol.OFFSET_SMALLEST
            )
        );
    }

    return options;
}


/**
 * Issues an offset request and calls setOffset with the returned value.
 * If topPar.time is not given, it will be set to the value of
 * self.options.autoOffsetReset. topPar shoould be of the form
 *  {
 *      topic: 't',
 *      partition: 0,
 *      time: ... # optional
 *  }
 */
function resetOffset(topPar) {
    var self = this;
    self.pause();

    // metadata is arbitrary (OTTO: ?)
    topPar.metadata = 'm';

    // If topPar.time is not provided, use the value of autoOffsetReset.
    topPar.time = topPar.time || self.options.autoOffsetReset;

    // We only need to request a single offset when resetting the
    // this consumer's topic-partition offset pointer.
    topPar.maxNum = 1;

    debug(
        "Resetting offset for %s %d to %d",
        topPar.topic, topPar.partition, topPar.time
    );

    self.offsetRequest([topPar], function (err, topParOffsets) {
        if (err) {
            self.emit('error', new errors.InvalidConsumerOffsetError(self));
        }
        else {
            // top_par_offsets will have an array of offsets.  We only
            // requested one offset so we know it the first element.
            var newOffset = topParOffsets[topPar.topic][topPar.partition][0]
            self.setOffset(topPar.topic, topPar.partition, newOffset);

            debug(
                "Reset offset for %s %d to %d",
                topPar.topic, topPar.partition, newOffset
            );
            self.resume();
        }
    });
}

module.exports = {
    validateConsumerOptions: validateConsumerOptions,
    resetOffset: resetOffset
}
