module Avram::BulkInsert(T)
  macro included
    define_import

    macro inherited
      define_import
    end
  end

  macro define_import
    def self.import(all_operations : Array(self))
      all_operations.each(&.before_save)

      invalid_operations = [] of self
      operations = [] of self
      all_operations.each do |op|
        if op.valid?
          operations << op
        else
          invalid_operations << op
        end
      end

      invalid_operations.each do |operation|
        operation.mark_as_failed

        Avram::Events::SaveFailedEvent.publish(
          operation_class: self.class.name,
          attributes: operation.generic_attributes
        )
      end

      now = Time.utc

      insert_values = operations.map do |operation|
        operation.created_at.value ||= now if operation.responds_to?(:created_at)
        operation.updated_at.value ||= now if operation.responds_to?(:updated_at)
        operation.values
      end

      insert_sql = Avram::Insert.new(T.table_name, insert_values, T.column_names)

      transaction_committed = T.database.transaction do
        T.database.query insert_sql.statement_for_bulk, args: insert_sql.args do |rs|
          begin
            T.from_rs(rs).each_with_index do |record, index|
              begin
                operation = operations[index]
                operation.record = record
                operation.after_save(record)
              rescue
                # trying to move to next valid record in the bulk
              end
            end
          rescue
            # swallow exception, possibly OverflowArithmetic on the Time.span in the PG decoder
            # not sure why it happens, but then this exception rollbacks the transaction and
            # every insert in the bulk is lot
            # side effect is that after_save won't run and record is lost, but if you don't need it
            # shouldn't be a problem, for now
          end
        end

        true
      end

      if transaction_committed
        operations.each do |operation|
          operation.save_status = OperationStatus::Saved
          if operation.record # the "do nothing" statement will not return anything
            operation.after_commit(operation.record.as(T))

            Avram::Events::SaveSuccessEvent.publish(
              operation_class: self.class.name,
              attributes: operation.generic_attributes
            )
          else
            operation.mark_as_failed
            Avram::Events::SaveFailedEvent.publish(
              operation_class: self.class.name,
              attributes: operation.generic_attributes
            )
          end
        end

        true
      else
        operations.each do |operation|
          operation.mark_as_failed

          Avram::Events::SaveFailedEvent.publish(
            operation_class: self.class.name,
            attributes: operation.generic_attributes
          )
        end

        false
      end
    end
  end
end
