#! /usr/bin/env ruby
#
# Copyright 2013 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'google/apis/datastore_v1beta2'

Datastore = Google::Apis::DatastoreV1beta2  # Alias the module

def create_service(dataset_id)
  service = Datastore::DatastoreService.new

  # Set authorization scopes and credentials.
  service.authorization = Google::Auth.get_application_default(
    # Set the credentials to have a readonly scope to the storage service.
    [Datastore::AUTH_DATASTORE, Datastore::AUTH_USERINFO_EMAIL]
  )

  service
end

def begin_transaction(service, dataset_id)
  # Start a new transaction.
  resp = service.begin_transaction(
    dataset_id, Datastore::BeginTransactionRequest.new)

  # Get the transaction handle
  resp.transaction
end

def lookup_entity(service, dataset_id, tx, path)
  # Get the entity by key.
  request = Datastore::LookupRequest.new(
    # Set the transaction, so we get a consistent snapshot of the
    # value at the time the transaction started.
    readOptions: {transaction: tx},
    # Add one entity key to the lookup request, with only one
    # :path element (i.e. no parent)
    keys: [{path: path}]
  )
  resp = service.lookup(dataset_id, request)

  resp.found
end

def insert_entity(service, dataset_id, tx, path, question, answer)
  entity = Datastore::Entity.new(
    # Set the entity key with only one `path` element: no parent.
    key: Datastore::Key.new(
      path: [Datastore::KeyPathElement.new(**path)]
    ),
    # Set the entity properties:
    # - a utf-8 string: `question`
    # - a 64bit integer: `answer`
    properties: {
      question: Datastore::Property.new(string_value: question),
      answer: Datastore::Property.new(integer_value: answer),
    }
  )
  # Build a mutation to insert the new entity.
  mutation = Datastore::Mutation.new(insert: [entity])

  # Commit the transaction and the insert mutation if the entity was not
  # found.
  request = Datastore::CommitRequest.new(
    transaction: tx,
    mutation: mutation
  )
  service.commit(dataset_id, request)
end

def ask_trivia(question, answer)
  # Print the question and read one line from stdin.
  printf '%s ', question
  result = STDIN.gets.chomp
  # Validate the input against the entity answer property.
  if result == answer.to_s
    puts ("Fascinating, extraordinary and, when you think hard about it, " +
          "completely obvious.")
  else
    puts "Don't Panic!"
  end
end

def delete_path(service, dataset_id, tx, path)
end

# Exercises the functions defined here
def main
  if ARGV.empty?
    abort "usage: adams.rb <dataset-id>"
  end

  # Get the dataset id from command line argument.
  dataset_id = ARGV[0]

  service = create_service(dataset_id)
  tx = begin_transaction(service, dataset_id)

  path = {kind: 'Trivia', name: 'hgtg42'}
  found = lookup_entity(service, dataset_id, tx, [path])
  if not found.empty?
    # Get the entity from the response if found.
    entity = found[0].entity
    # Get `question` property value.
    question = entity.properties['question'].string_value
    # Get `answer` property value.
    answer = entity.properties['answer'].integer_value
  else
    question = 'Meaning of life?'
    answer = 42
    insert_entity(service, dataset_id, tx, path, question, answer)
  end

  ask_trivia(question, answer)

  # clean up
  delete_path(service, dataset_id, tx, path)
end

# If running this file as a script, execute the main function
if __FILE__ == $0
  main()
end
