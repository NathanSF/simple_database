#!/usr/bin/python

# Simple Database
# Nathan Frankel
# Python 2.7

from collections import defaultdict


class Database(object):
    """Simple in memory database

    Commands:
    GET, SET, UNSET, NUMEQUALTO, END

    Transactional commands:
    BEGIN, ROLLBACK, COMMIT
    """

    def __init__(self):
        """
        Initialize database
        """
        # Database is a dictionary so average case runtime is O(1)
        # for set, get, and unset
        self.database = {}

        # Keep count of occurance for each key
        self.value_count = defaultdict(int)

        # List for pending transactions, last dict in list
        # is current transaction
        self.pending_transactions_list = []

    def set(self, key, value, commit=False):
        """
        Set database value for key, or add set command to latest
        pending transaction block
        """
        orig_val = self.database.get(key)
        if self.pending_transactions_list and not commit:
            self.pending_transactions_list[-1][key] = value
        else:
            self.database[key] = value
        # Update counts if not committing. If committing counts
        # are already correct
        if not commit:
            self.value_count[value] += 1
            if orig_val and orig_val != value:
                self.value_count[orig_val] -= 1

    def get(self, key):
        """
        Print value for key from database if exists, else print NULL.
        If we have pending transactions search these first.
        """
        if self.pending_transactions_list:
            for transaction in reversed(self.pending_transactions_list):
                if key in transaction:
                    if transaction[key] is None:
                        print 'NULL'
                    else:
                        print transaction[key]
                    # Value found in latest transaction, exit method
                    return
        if key in self.database:
            print self.database[key]
        else:
            print "NULL"

    def unset(self, key, commit=False):
        """
        Unset key from database, or add unset command to latest
        pending transaction block
        """
        orig_val = self.database.get(key)
        if self.pending_transactions_list and not commit:
            self.pending_transactions_list[-1][key] = None
        elif orig_val:
            del self.database[key]
        # Decerement counter for this value unless we are committing.
        # Check for count greater than 0 to account for case of
        # consecutive unset statements for the same key.
        if not commit and orig_val and self.value_count[orig_val] > 0:
            self.value_count[orig_val] -= 1

    def numequalto(self, value):
        """
        Print count of keys equal to value
        """
        if value in self.value_count:
            print self.value_count[value]
        else:
            print 0

    def begin(self):
        """
        Open new transaction block. Append to list O(1) time complexity.
        """
        self.pending_transactions_list.append({})

    def rollback(self):
        """
        Undo commands in most recent transaction block. Print nothing if
        successful, NO TRANSACTION if no transaction in progress.
        """
        if self.pending_transactions_list:
            transaction = self.pending_transactions_list.pop()

            keys_to_update = []
            for key in transaction:
                keys_to_update.append(key)

            # Only update count if this key not changed in another
            # transaction block
            for pending in self.pending_transactions_list:
                for pending_key in pending:
                    if pending_key in keys_to_update:
                        keys_to_update.remove(pending_key)

            # keys_to_update now contains keys not changed
            # in remaining transaction blocks
            for key in keys_to_update:
                orig_val = self.database.get(key)
                # If orig val in database increment
                if orig_val:
                    self.value_count[self.database[key]] += 1
                if transaction[key] is not None:
                    # Rolling back set so decrement count
                    self.value_count[transaction[key]] -= 1
        else:
            print 'NO TRANSACTION'

    def commit(self):
        """
        Close all open transaction blocks and apply changes. Print nothing if
        successful, or NO TRANSACTION if no transaction is in progress.
        """
        if self.pending_transactions_list:
            # Iterate over reversed and commit all transactions
            # Keep track of keys updated so we only update once
            keys_processed = []
            for transaction in reversed(self.pending_transactions_list):
                keys = transaction.keys()
                # Iterate over the keys in this transaction
                for key in keys:
                    if key in keys_processed:
                        continue
                    if transaction[key] is None:
                        self.unset(key, True)
                    else:
                        self.set(key, transaction[key], True)
                    keys_processed.append(key)
            # reset pending transactions to empty list
            self.pending_transactions_list = []
        else:
            print 'NO TRANSACTION'

if __name__ == "__main__":
    data = Database()

    line = raw_input().strip()

    # We're going to allow upper and lower case commands for this exercise
    while line not in ['END', 'end']:
        # Split line on space to get command, key, value
        argument_row = line.split(' ')

        command = argument_row[0]

        valid_command_list = ['set', 'get', 'unset', 'numequalto', 'begin',
                              'rollback', 'commit']

        # Make sure we have a valid command
        command = command.lower()
        if command in valid_command_list:
            # Call appropriate Database method
            db_method = getattr(Database, command)
            try:
                db_method(data, *argument_row[1:])
            except (TypeError):
                print "Invalid command (check number of arguments)"
        else:
            print "Enter a valid command if you want to do something"

        line = raw_input().strip()
