import {InvalidFieldValueError} from '../core/error';
import {YouTubeVideoListQuery} from './list';
import {YouTubeId} from './id';

export function main() {
    describe('YouTubeVideoListQuery', function () {
        it('should validateQuery to have a valid part/parts.', function () {
            var query = new YouTubeVideoListQuery();
            query.key('1231');

            query.equal('part', 'snippet');
            expect(() => query.validateQuery()).not.toThrow(new InvalidFieldValueError('part', 'monkey'));

            query.equal('part', 'monkey');
            expect(() => query.validateQuery()).toThrow(new InvalidFieldValueError('part', 'monkey'));
        });

        it('should add ids to list query.', function () {
            var query = new YouTubeVideoListQuery();
            query.key('42');

            query.id(new YouTubeId('1'));
            expect(query.params).toEqual({'key': ['42'], 'id': ['1']});

            query.id([new YouTubeId('2'), new YouTubeId('3')]);
            expect(query.params).toEqual({'key': ['42'], 'id': ['1', '2', '3']});
        });
    });
}