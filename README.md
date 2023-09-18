# voluble

> :warning: **Voluble is now deprecated, see http://shadowtraffic.io**
>
> I've unfortunately run out of time to maintain this open-source connector. In its place, I've launched an improved commercial offering so that I can sustainably provide a similar service. Thanks for understanding!

[![CircleCI](https://circleci.com/gh/MichaelDrogalis/voluble.svg?style=svg)](https://circleci.com/gh/MichaelDrogalis/voluble)

When working with Apache Kafka, you often find yourself wanting to continuously populate your topics with something that approximates the shape of your production data. Voluble solves that problem. The primary things it supports are:

- Creating realistic data by integrating with [Java Faker](https://github.com/DiUS/java-faker). 
  - To get an idea of the entities that JavaFaker can create, see [this example site](https://java-faker.herokuapp.com/).
- Cross-topic relationships
- Populating both keys and values of records
- Making both primitive and complex/nested values
- Bounded or unbounded streams of data
- Tombstoning

Voluble ships as a [Kafka connector](https://docs.confluent.io/current/connect/index.html) to make it easy to scale and change serialization formats. You can use Kafka Connect through its REST API or integrated with [ksqlDB](http://ksqldb.io/). In this guide, I demonstrate using the latter, but the configuration is the same for both. I leave out Connect specific configuration like serializers and tasks that need to be configured for any connector.

## Installation

Install the Connector with [`confluent-hub`](https://docs.confluent.io/current/connect/managing/confluent-hub/client.html):

```
confluent-hub install mdrogalis/voluble:0.3.1
```

If you want to use it as a library:

```
[io.mdrogalis/voluble "0.3.1"]
```

## Quick example

Let's dive right in and look at an example. This exercises a bunch of features to give you an idea of what it's like to work with Voluble:

```sql
CREATE SOURCE CONNECTOR s WITH (
  'connector.class' = 'io.mdrogalis.voluble.VolubleSourceConnector',

  'genkp.owners.with' = '#{Internet.uuid}',
  'genv.owners.name->full.with' = '#{Name.full_name}',
  'genv.owners.creditCardNumber.with' = '#{Finance.credit_card}',

  'genk.cats.name.with' = '#{FunnyName.name}',
  'genv.cats.owner.matching' = 'owners.key',

  'genk.diets.catName.matching' = 'cats.key.name',
  'genv.diets.dish.with' = '#{Food.vegetables}',
  'genv.diets.measurement.with' = '#{Food.measurements}',
  'genv.diets.size.with' = '#{Food.measurement_sizes}',

  'genk.adopters.name.sometimes.with' = '#{Name.full_name}',
  'genk.adopters.name.sometimes.matching' = 'adopters.key.name',
  'genv.adopters.jobTitle.with' = '#{Job.title}',
  'attrk.adopters.name.matching.rate' = '0.05',
  'topic.adopters.tombstone.rate' = '0.10',

  'global.throttle.ms' = '500',
  'global.history.records.max' = '100000'
);
```

This example generates data for 4 topics: `owners`, `cats`, `diets`, and `adopters`. 

* The `owners` topic consists of records with primitive UUID keys and complex values (`name` and `creditCardNumber`). These values are generated through Java Faker expressions. 
* The value of the events for `cats` has a field named `owner` which has to match one of the UUID generated in the `owner` topic. 
* In the `diets` topic, you can see a similar property set for the key, except this one grabs a value from a complex key (`name` in the key of the `cats` topic). 
* The `adopters` topic has keys that are sometimes new, but sometimes repeated (`sometimes.matching` is running against the same topic as its specified for). This is basically a nice easy to represent mutation. A tombstone record is generated for this topic `10%` of the time. 

Lastly, Voluble will only generate a new record per topic every ~`500` ms and keep at most `100000` records of history in memory per topic to perform all this matching against.

When you run this connector, you'll get data looking roughly like the following:

```json

[
    {
        "topic": "owners",
        "event": {
            "key": "57da6bd0-dc33-4c2d-9a3f-046d2a008085",
            "value": {
                "name": {
                    "full": "Rene Bashirian"
                },
                "creditCardNumber": "3680-695522-0973"
            }
        }
    },
    {
        "topic": "cats",
        "event": {
            "key": {
                "name": "Kenny Dewitt"
            },
            "value": {
                "owner": "57da6bd0-dc33-4c2d-9a3f-046d2a008085"
            }
        }
    },
    {
        "topic": "diets",
        "event": {
            "key": {
                "name": "Kenny Dewitt"
            },
            "value": {
                "dish": "Celery",
                "measurement": "teaspoon",
                "size": "1/3"
            }
        }
    },
    {
        "topic": "adopters",
        "event": {
            "key": {
                "name": "Sheldon Feeney"
            },
            "value": {
                "jobTitle": "IT Producer"
            }
        }
    }
]
```

## Usage

All configuration for Voluble is done through properties that are passed to the connector.

### Generating data

Generating events with Voluble basically take the form of:

```
'<directive>.<topic>.[attribute?].[qualifier?].<generator>' = 'expression'
```

Let's break down what exactly that means. 

#### Directives

There are 4 top-level directives for generating a single Kafka record: `genk`, `genkp`, `genv`, and `genvp`. Those stand for generate key, generate key primitive, generate value, and generate value primitive. Non-primitive, or complex, generators are for generating maps of values where the keys are named. Key and value generators can be used together in any combination, but primitive and complex generators for the same part of a record, like the key, are mutually exclusive.

#### Topic and attribute

Topic is the topic that Voluble will generate data to for this expression. If you're generating a complex value, you'll also need to specify an attribute, which is just the key that the expression will be generated for. For example, you might specify that an attribute is `"name"`. When you generate events, you'll get maps with a `"name"` key in them.

#### Generator

There are two types of generators: `with` and `matching`. `with` takes a Java Faker expression as its value and generates data irrespective of any other topics. `matching` allows you to generate data that has already been generated in another topic. This is useful when your real data has relationships and might be joined downstream. The value for a `matching` generator takes the form of: `<topic>.[key|value].[attribute?]`. This syntax let's you target data in another topic in either the key or value of a record. If it's a complex data structure, you can get a single key out of it.

`matching` is "stable", in that for every iteration, Voluble selects a single event from another topic and runs all matches against that.

#### Qualifier

Qualifiers let you control how generators work. Right now there is only one qualifier: `sometimes`. Sometimes you want to generate data that matches another topic, but not always. This is useful if you're modeling a single topic who's key's represent mutability. Or maybe you want to model a stream/stream join. `sometimes` allows you to control the probability that Voluble will generate a matching value versus a brand new one. You'd use it roughly like the following: `genv.users.team.sometimes.matching` = `team.key.name`, `genv.users.team.sometimes.with` = `'#{Team.name}'`. When you use `sometimes`, you need to specify both `matching` and `with`. By default there is now a `0.1` probability rate of matching, instead of `1`. You can control the probability to suit your circumstances (see the configuration section).

#### Expressions

When a `with` generator is used, the value is passed verbatim to Java Faker to create a value. Java Faker has a huge number of categories that it can generate data for. Just check out the project to get a sense for what you can do. Under the covers, the [`expression` method](https://github.com/DiUS/java-faker/blob/7ac7e53aa2e9a3d39c1e663ddf62b1feb625b060/src/main/java/com/github/javafaker/Faker.java#L636-L654) of Faker is being invoked to dynamically create data without going through its Java classes.

Some Faker categories take parameters as arguments. The parser for arguments is rather strict: all values surrounded with apostrophes, and no spaces in between each value. Some arguments (like TimeUnit) appear to be case-sensitive. Here are some examples:

```
#{number.number_between '-9','9'}
#{date.birthday}
#{date.birthday '10','20'}
#{date.past '10','DAYS'}
#{date.between 'Sun Mar 22 01:59:02 PDT 2020','Sun Mar 24 01:59:02 PDT 2020'}
```

If you get stuck generating something that you want, just instantiate Faker directly in a Java program and call `faker.expression()` until you get the thing you're looking for.

## Nesting

You can nest data and access nested data in other events with arrow syntax (`->`). You can use this to generate data, as in `genv.owners.name->full.with` = `#{Name.full_name}`, which will create maps like `{"name": {"full": "Rene Bashirian"}}`. You can also reference nested values in a matching statement, as in `genv.cats.owner.matching` = `owners.value.name->full`. Arrow syntax can be used anywhere that an attribute is allowed.

## More examples

For concision, I just list out the relevant configuration.

**A primitive key and primitive value**

```
'genkp.people.with' = '#{Internet.uuid}'
'genvp.people.with' = '#{Name.full_name}'
```

**A primitive key and complex value**

```
'genkp.people.with' = '#{Internet.uuid}'
'genv.people.name.with' = '#{Name.full_name}'
'genv.people.bloodType.with' = '#{Name.blood_group}'
```

**A complex key and complex value**

```
'genk.people.id.with' = '#{Internet.uuid}'
'genk.people.avatar.with' = '#{Internet.avatar}'
'genv.people.name.with' = '#{Name.full_name}'
'genv.people.bloodType.with' = '#{Name.blood_group}'
```

**A topic that gets part of its value from another topic's primitive key**

`key` can instead be `value` to reference that part of the record.

```
'genkp.users.with' = '#{Name.full_name}'
'genvp.users.with' = '#{Name.blood_group}'

'genkp.publications.matching' = 'users.key'
'genv.publications.title.with' = '#{Book.title}'
```

**A topic that gets part of its value from another topic's complex key**

```
'genk.users.name.with' = '#{Name.full_name}'
'genvp.users.with' = '#{Name.blood_group}'

'genkp.publications.matching' = 'users.key.name'
'genv.publications.title.with' = '#{Book.title}'
```

**A topic with keys that update once in a while**

Notice that a record can match against itself. Useful for representing updates in a topic.

```
'genkp.users.sometimes.with' = '#{Name.full_name}'
'genkp.users.sometimes.matching' = 'users.key'
'genv.users.bloodType.with' = '#{Name.blood_group}'
```

**Two topics that sometimes share a key in common**

Useful for modeling stream/stream joins.

```
'genkp.teamA.sometimes.with' = '#{Team.name}'
'genkp.teamA.sometimes.matching' = 'teamB.key'

'genkp.teamB.sometimes.with' = '#{Team.name}'
'genkp.teamB.sometimes.matching' = 'teamA.key'
```

**Creating nested data and accessing it from a match**

```
'genv.teamA.stadium->location.with' = '#{Address.state}'
'genv.teamB.backupLocation.matching' = 'teamA.value.stadium->location'
```

## Configuration

Voluble has a few other knobs for controlling useful properties. Some properties can be defined at the attribute, topic, and global level. The most granular scope takes precedence (topic over global, etc).

### Throttling

By default, Voluble will try to generate data to Kafka as fast as possible. But sometimes this is simply too much. You can throttle how fast data is generated with `global.throttle.ms`, or per topic with `topic.<topic>.throttle.ms`.

### Matching probability

When you use the `sometimes` qualifier, the rate of matching is reduced from a certainty (`1`) to `0.1`. You can control this probability at both the global and attribute level. To alter the matching rate global, configure `global.matching.rate` to be a value between `0` and `1`. To configure it at the attribute level, configure it roughly like `attrk.<topic>.<attribute>.matching.rate`. Attribute configuration mirrors generator configuration: `attrk`, `attrkp`, `attrv`, and `attrvp` the same as generators.

### Tombstoning

In Kafka, a key with a `null` value is called a tombstone. It conventionally represents the deletion of a key in a topic. Sometimes it's useful to generate tombstones. By default Voluble won't do this, but you can turn it on per topic with `topic.<topic>.tombstone.rate` = `p`. `p` is a value between `0` and `1`, and it represents the probability that a tombstone will be generated instead of a value.

### Null value rates

Sometimes when you're generating complex values, you might want the value for a key to be null. By default Voluble doesn't do this, but you can turn configure it at the attribute level like so: `attrv.<topic>.<attribute>.null.rate` = `p`.

### Bounded event streams

There are some cases where it's realistic to produce exactly n records to a topic, no more and no less. This might be the case if you're loading a table of static reference data into Kafka. You can instruct Voluble to do this with the form `topic.<topic>.records.exactly` = `n`, where n is an integer > `0`.

### History capacity

To perform `matching` expressions, Voluble needs to keep the history of previously generated records for each topic. By default, only the most recent `1,000,000` records are kept per topic. You can override this per topic with `topic.<topic>.history.records.max` = `n`, or globally with `global.history.records.max` = `n`.

## Reference

| Key form  | Value form | Default value | Meaning |
| --------- | ---------- | ------------- | ------- |
| `(genkp\|genvp).<topic>.with` | `#{expr}` | unset | Makes a new record and sends it to `<topic>`. Evaluates the Java Faker expression and supplies it as a primitive type for the key or value. |
| `(genk\|genv).<topic>.<attr>.with` | `#{expr}` | unset | Makes a new record and sends it to `<topic>`. Evaluates the Java Faker expression and supplies it as a map for the key or value. The map has form: `<attr>` -> Java Faker expr. |
| `(genkp\|genvp).<topic>.matching` | `<src>.(key\|value).[attr?]` | unset | Makes a new record and sends it to `<topic>`. The key or value of the record is derived from a previous record produced to topic `<src>`'s key or value. Can optionally drill into an attribute of the resulting value. The value will be supplied as a primitive value to the new record. |
| `(genk\|genv).<topic>.<attr>.matching` | `<src>.(key\|value).[attr?]` | unset | Makes a new record and sends it to `<topic>`. The key or value of the record is derived from a previous record produced to topic `<src>`'s key or value. Can optionally drill into an attribute of the resulting value. The value will be a map with form:  `<attr>` -> derived value. |
| `(genkp\|genvp).<topic>.sometimes.with` | `#{expr}` | unset | Makes a new record and sends it to `<topic>`. Same semantics as `with` without `sometimes`. Will be chosen according to the specified matching rate. When present, `sometimes.matching` must also be included. |
| `(genk\|genv).<topic>.<attr>.sometimes.with` | `#{expr}` | unset | Makes a new record and sends it to `<topic>`. Same semantics as `with` without `sometimes`. Will be chosen according to the specified matching rate. When present, `sometimes.matching` must also be included. |
| `(genkp\|genvp).<topic>.sometimes.matching` | `<src>.(key\|value).[attr?]` | unset | Makes a new record and sends it to `<topic>`. Same semantics as `matching` without `sometimes`. Will be chosen according to the specified matching rate. When present, `sometimes.with` must also be included. |
| `(genk\|genv).<topic>.<attr>.sometimes.matching` | `<src>.(key\|value).[attr?]` | unset | Makes a new record and sends it to `<topic>`. Same semantics as `matching` without `sometimes`. Will be chosen according to the specified matching rate. When present, `sometimes.with` must also be included. |
| `global.throttle.ms` | `<long>` | `0` | Wait at least this number of milliseconds before producing a new record for each topic. |
| `topic.<topic>.throttle.ms` | `<long>` | unset | Wait at least this number of milliseconds before producing a new record for `<topic>`. Overrides its global sibling. |
| `topic.<topic>.tombstone.rate` | `<double>` | `0` | Probability of producing a record to `<topic>` with a tombstone (null) value. Must be a value between `0` and `1`. |
| `(attrkp\|attrvp).<topic>.null.rate` | `<double>` | `0` | Probability of producing a record to `<topic>` with key or value as null. Must be a value between `0` and `1`. |
| `(attrk\|attrv).<topic>.<attr>.null.rate` | `<double>` | `0` | Probability of producing a record to `<topic>` the key or value as null. Must be a value between `0` and `1`. |
| `global.history.records.max` | `<long>` | `1000000` | The maximum number of records to remember for matching for each topic. The oldest records will be deleted first. |
| `topic.<topic>.history.records.max` | `<long>` | unset | The maximum number of records to remember for matching for `<topic>`. The oldest records will be deleted first. Overrides its global sibling. |
| `global.matching.rate` | `<double>` | `0.1` | Probability of matching when `sometimes` is used anywhere. Must be a value between `0` and `1`. |
| `(attrkp\|attrvp).<topic>.matching.rate` | `<double>` | unset | Probability of matching when `sometimes` is used for `<topic>`. Must be a value between `0` and `1`. Overrides its global sibling. |
| `(attrk\|attrv).<topic>.<attr>.matching.rate` | `<double>` | unset | Probability of matching when `sometimes` is used for `<topic>` with `<attr>`. Must be a value between `0` and `1`. Overrides its global sibling. |
| `topic.<topic>.records.exactly` | `<long>` | unset | Generates exactly this many records for `<topic>`. `<topic>` will be retired from generating new records after this count is reached. |

## Limitations

- There is no built-in support for multi-schema topics. If you want to do this, just create multiple instances of Voluble with different generator configurations.
- Voluble doesn't yet validate that you're not asking for something impossible, like a circular dependency. If it's internal state machine can't advance and generate a new event after `100` iterations, the connector will permanently abort.

If Voluble doesn't do something you want it to, feel free to open and issue or submit a patch.

## Things to add in the future

- Maximal unique values: `within` generates at most N unique values.
- Distributions: `matching` selects with a uniform, normal, etc distribution

## License

Copyright © 2020 Michael Drogalis

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
