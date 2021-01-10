from schema import Schema, And, Use

record_schema = Schema({
    'id': And(Use(str)),
    'data': {
        'id': And(Use(str)),
        'organization': And(Use(str)),
        'credit_score': And(Use(int)),
        'amount': And(Use(int)),
        'country': And(Use(str)),
    }
})
