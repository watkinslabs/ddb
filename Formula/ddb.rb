class Ddb < Formula
  desc "Enterprise SQL engine for file-based analytics"
  homepage "https://watkinslabs.github.io/ddb/"
  url "https://github.com/watkinslabs/ddb/archive/refs/tags/v1.0.0.tar.gz"
  sha256 "replace-with-actual-sha256"
  license "MIT"
  head "https://github.com/watkinslabs/ddb.git", branch: "main"

  depends_on "go" => :build

  def install
    system "go", "build", *std_go_args(ldflags: "-s -w"), "-o", bin/"ddb"
    
    # Install shell completions
    generate_completions_from_executable(bin/"ddb", "completion")
    
    # Install man page if available
    if (buildpath/"docs/ddb.1").exist?
      man1.install "docs/ddb.1"
    end
  end

  test do
    # Create a test CSV file
    (testpath/"test.csv").write <<~EOS
      name,age,city
      Alice,25,New York
      Bob,30,San Francisco
      Charlie,35,Chicago
    EOS

    # Test basic query functionality
    output = shell_output("#{bin}/ddb query 'SELECT name FROM data WHERE age > 25' --file data:#{testpath}/test.csv")
    assert_match "Bob", output
    assert_match "Charlie", output
    refute_match "Alice", output

    # Test version command
    assert_match "ddb version", shell_output("#{bin}/ddb version")
    
    # Test help command
    assert_match "Enterprise SQL Engine", shell_output("#{bin}/ddb --help")
  end
end