require 'rspec'
describe 'database' do
    def run_script(commands)

      raw_output = nil
      IO.popen("./db2", "r+") do |pipe|
        commands.each do |command|
          pipe.puts command
        end

        pipe.close_write

        # Read entire output
        raw_output = pipe.gets(nil)
      end
      raw_output.split("\n")
    end

    it 'prints error message when table is full' do
      script = (1..1400).map do |i|
        "insert #{i} user#{i} person#{i}@example\n"
      end
      script << ".exit"
      # p script
      result = run_script(script)
      expect(result[-2]).to eq("db > Error: Table full.")
      # expect(result[-1]).to eq(".exit")
    end
  end
