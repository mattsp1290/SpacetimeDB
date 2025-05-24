// Package errors provides comprehensive error documentation and cataloging for SpacetimeDB Go bindings.
// This module implements auto-generated error catalogs, troubleshooting guides,
// best practices documentation, and API documentation integration.
package errors

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrorDocumentation represents comprehensive error documentation
type ErrorDocumentation struct {
	ErrorCode       string                `json:"error_code"`
	Name            string                `json:"name"`
	Description     string                `json:"description"`
	Category        string                `json:"category"`
	Severity        string                `json:"severity"`
	Causes          []string              `json:"causes"`
	Solutions       []string              `json:"solutions"`
	Examples        []ErrorExample        `json:"examples"`
	RelatedCodes    []string              `json:"related_codes"`
	References      []Reference           `json:"references"`
	BestPractices   []string              `json:"best_practices"`
	Troubleshooting []TroubleshootingStep `json:"troubleshooting"`
	LastUpdated     time.Time             `json:"last_updated"`
}

// ErrorExample represents an example of an error occurrence
type ErrorExample struct {
	Context  string                 `json:"context"`
	Code     string                 `json:"code"`
	Message  string                 `json:"message"`
	Solution string                 `json:"solution"`
	Metadata map[string]interface{} `json:"metadata"`
}

// Reference represents a documentation reference
type Reference struct {
	Title       string `json:"title"`
	URL         string `json:"url"`
	Description string `json:"description"`
	Type        string `json:"type"` // "documentation", "tutorial", "api", "forum"
}

// TroubleshootingStep represents a step in troubleshooting guide
type TroubleshootingStep struct {
	Step        int    `json:"step"`
	Description string `json:"description"`
	Command     string `json:"command,omitempty"`
	Expected    string `json:"expected,omitempty"`
	IfFails     string `json:"if_fails,omitempty"`
}

// ErrorCatalog manages a catalog of error documentation
type ErrorCatalog struct {
	errors    map[string]*ErrorDocumentation
	templates map[string]*template.Template
	mutex     sync.RWMutex
}

// NewErrorCatalog creates a new error catalog
func NewErrorCatalog() *ErrorCatalog {
	catalog := &ErrorCatalog{
		errors:    make(map[string]*ErrorDocumentation),
		templates: make(map[string]*template.Template),
	}

	catalog.initializeDefaultDocumentation()
	catalog.initializeTemplates()

	return catalog
}

// AddErrorDocumentation adds error documentation to the catalog
func (ec *ErrorCatalog) AddErrorDocumentation(doc *ErrorDocumentation) {
	ec.mutex.Lock()
	defer ec.mutex.Unlock()

	doc.LastUpdated = time.Now()
	ec.errors[doc.ErrorCode] = doc
}

// GetErrorDocumentation retrieves error documentation by code
func (ec *ErrorCatalog) GetErrorDocumentation(errorCode string) (*ErrorDocumentation, bool) {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	doc, exists := ec.errors[errorCode]
	return doc, exists
}

// GetDocumentationByCategory returns all documentation for a category
func (ec *ErrorCatalog) GetDocumentationByCategory(category string) []*ErrorDocumentation {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	var docs []*ErrorDocumentation
	for _, doc := range ec.errors {
		if doc.Category == category {
			docs = append(docs, doc)
		}
	}

	return docs
}

// SearchDocumentation searches for documentation by keyword
func (ec *ErrorCatalog) SearchDocumentation(query string) []*ErrorDocumentation {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	query = strings.ToLower(query)
	var results []*ErrorDocumentation

	for _, doc := range ec.errors {
		if ec.matchesQuery(doc, query) {
			results = append(results, doc)
		}
	}

	// Sort by relevance (simple name match first, then description)
	sort.Slice(results, func(i, j int) bool {
		iNameMatch := strings.Contains(strings.ToLower(results[i].Name), query)
		jNameMatch := strings.Contains(strings.ToLower(results[j].Name), query)

		if iNameMatch && !jNameMatch {
			return true
		}
		if !iNameMatch && jNameMatch {
			return false
		}

		return results[i].Name < results[j].Name
	})

	return results
}

// matchesQuery checks if documentation matches a search query
func (ec *ErrorCatalog) matchesQuery(doc *ErrorDocumentation, query string) bool {
	searchableText := strings.ToLower(fmt.Sprintf("%s %s %s %s",
		doc.Name, doc.Description, doc.Category, strings.Join(doc.Causes, " ")))

	return strings.Contains(searchableText, query)
}

// GenerateHTMLCatalog generates an HTML catalog of all errors
func (ec *ErrorCatalog) GenerateHTMLCatalog(writer io.Writer) error {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	tmpl, exists := ec.templates["catalog"]
	if !exists {
		return fmt.Errorf("catalog template not found")
	}

	// Group errors by category
	categories := make(map[string][]*ErrorDocumentation)
	for _, doc := range ec.errors {
		categories[doc.Category] = append(categories[doc.Category], doc)
	}

	// Sort categories and errors within each category
	var sortedCategories []string
	for category := range categories {
		sortedCategories = append(sortedCategories, category)
	}
	sort.Strings(sortedCategories)

	for _, docs := range categories {
		sort.Slice(docs, func(i, j int) bool {
			return docs[i].Name < docs[j].Name
		})
	}

	data := map[string]interface{}{
		"Title":         "SpacetimeDB Go Bindings Error Catalog",
		"Generated":     time.Now().Format("2006-01-02 15:04:05"),
		"Categories":    categories,
		"CategoryOrder": sortedCategories,
		"TotalErrors":   len(ec.errors),
	}

	return tmpl.Execute(writer, data)
}

// GenerateMarkdownCatalog generates a Markdown catalog of all errors
func (ec *ErrorCatalog) GenerateMarkdownCatalog(writer io.Writer) error {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	tmpl, exists := ec.templates["markdown"]
	if !exists {
		return fmt.Errorf("markdown template not found")
	}

	// Group errors by category
	categories := make(map[string][]*ErrorDocumentation)
	for _, doc := range ec.errors {
		categories[doc.Category] = append(categories[doc.Category], doc)
	}

	data := map[string]interface{}{
		"Title":       "SpacetimeDB Go Bindings Error Catalog",
		"Generated":   time.Now().Format("2006-01-02 15:04:05"),
		"Categories":  categories,
		"TotalErrors": len(ec.errors),
	}

	return tmpl.Execute(writer, data)
}

// GenerateJSONCatalog generates a JSON catalog of all errors
func (ec *ErrorCatalog) GenerateJSONCatalog(writer io.Writer) error {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	catalog := map[string]interface{}{
		"title":        "SpacetimeDB Go Bindings Error Catalog",
		"generated":    time.Now().Format(time.RFC3339),
		"total_errors": len(ec.errors),
		"errors":       ec.errors,
	}

	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(catalog)
}

// GenerateTroubleshootingGuide generates a troubleshooting guide for a specific error
func (ec *ErrorCatalog) GenerateTroubleshootingGuide(errorCode string, writer io.Writer) error {
	doc, exists := ec.GetErrorDocumentation(errorCode)
	if !exists {
		return fmt.Errorf("no documentation found for error code: %s", errorCode)
	}

	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	tmpl, exists := ec.templates["troubleshooting"]
	if !exists {
		return fmt.Errorf("troubleshooting template not found")
	}

	data := map[string]interface{}{
		"Error":     doc,
		"Generated": time.Now().Format("2006-01-02 15:04:05"),
	}

	return tmpl.Execute(writer, data)
}

// initializeDefaultDocumentation sets up default error documentation
func (ec *ErrorCatalog) initializeDefaultDocumentation() {
	defaultDocs := []*ErrorDocumentation{
		{
			ErrorCode:   "0x0001",
			Name:        "No Such Iterator",
			Description: "The specified iterator does not exist or has been invalidated",
			Category:    "Runtime",
			Severity:    "Medium",
			Causes: []string{
				"Iterator was closed or exhausted",
				"Invalid iterator handle passed to function",
				"Iterator expired due to transaction rollback",
			},
			Solutions: []string{
				"Verify iterator is still valid before use",
				"Create a new iterator if the previous one was exhausted",
				"Check if the transaction is still active",
			},
			Examples: []ErrorExample{
				{
					Context:  "Table iteration",
					Code:     "iter, _ := db.Scan(tableID); iter.Close(); iter.Read(buf)",
					Message:  "no such iterator",
					Solution: "Create a new iterator instead of reusing closed one",
				},
			},
			Troubleshooting: []TroubleshootingStep{
				{
					Step:        1,
					Description: "Check if iterator is still valid",
					Command:     "if !iter.IsExhausted() { ... }",
					Expected:    "Iterator should return valid status",
				},
				{
					Step:        2,
					Description: "Verify transaction state",
					Expected:    "Transaction should be active",
					IfFails:     "Start a new transaction",
				},
			},
		},
		{
			ErrorCode:   "0x0002",
			Name:        "Buffer Too Small",
			Description: "The provided buffer is insufficient for the requested operation",
			Category:    "Validation",
			Severity:    "Low",
			Causes: []string{
				"Buffer size is smaller than required data",
				"Incorrect buffer size calculation",
				"Data size changed between calls",
			},
			Solutions: []string{
				"Increase buffer size to accommodate data",
				"Query for required size before allocation",
				"Use dynamic buffer resizing",
			},
			BestPractices: []string{
				"Always check return values for size requirements",
				"Use growing buffers for unknown data sizes",
				"Implement proper error handling for buffer operations",
			},
		},
		{
			ErrorCode:   "0x0006",
			Name:        "BSATN Decode Error",
			Description: "Failed to decode Binary SpacetimeDB Algebraic Type Notation data",
			Category:    "Serialization",
			Severity:    "High",
			Causes: []string{
				"Corrupted BSATN data",
				"Mismatched schema versions",
				"Invalid data format",
				"Incomplete data transmission",
			},
			Solutions: []string{
				"Verify data integrity using checksums",
				"Ensure schema compatibility between client and server",
				"Validate data format before deserialization",
				"Implement proper error handling for network transmission",
			},
			References: []Reference{
				{
					Title:       "BSATN Format Specification",
					URL:         "https://spacetimedb.com/docs/internals/bsatn-data-format",
					Description: "Complete specification of the BSATN data format",
					Type:        "documentation",
				},
			},
		},
		{
			ErrorCode:   "0x0007",
			Name:        "Memory Exhausted",
			Description: "System has run out of available memory for the operation",
			Category:    "System",
			Severity:    "Critical",
			Causes: []string{
				"Insufficient system memory",
				"Memory leaks in application",
				"Large data operations without proper chunking",
				"Excessive concurrent operations",
			},
			Solutions: []string{
				"Increase available system memory",
				"Implement proper memory management and cleanup",
				"Use streaming operations for large datasets",
				"Limit concurrent operations",
				"Profile application for memory leaks",
			},
			BestPractices: []string{
				"Monitor memory usage in production",
				"Implement circuit breakers for memory-intensive operations",
				"Use memory pools for frequent allocations",
				"Set appropriate limits on operation sizes",
			},
		},
	}

	for _, doc := range defaultDocs {
		ec.errors[doc.ErrorCode] = doc
	}
}

// initializeTemplates sets up documentation templates
func (ec *ErrorCatalog) initializeTemplates() {
	// HTML catalog template
	htmlCatalogTmpl := `<!DOCTYPE html>
<html>
<head>
    <title>{{.Title}}</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .category { margin-bottom: 30px; }
        .error { margin-bottom: 20px; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }
        .error-code { font-weight: bold; color: #d73027; }
        .severity-critical { border-left: 5px solid #d73027; }
        .severity-high { border-left: 5px solid #fc8d59; }
        .severity-medium { border-left: 5px solid #fee08b; }
        .severity-low { border-left: 5px solid #91cf60; }
        .causes, .solutions { margin-top: 10px; }
        ul { padding-left: 20px; }
    </style>
</head>
<body>
    <h1>{{.Title}}</h1>
    <p>Generated: {{.Generated}}</p>
    <p>Total Errors: {{.TotalErrors}}</p>
    
    {{range .CategoryOrder}}
    <div class="category">
        <h2>{{.}}</h2>
        {{range index $.Categories .}}
        <div class="error severity-{{.Severity | lower}}">
            <h3><span class="error-code">{{.ErrorCode}}</span> - {{.Name}}</h3>
            <p>{{.Description}}</p>
            <div class="causes">
                <strong>Common Causes:</strong>
                <ul>
                {{range .Causes}}<li>{{.}}</li>{{end}}
                </ul>
            </div>
            <div class="solutions">
                <strong>Solutions:</strong>
                <ul>
                {{range .Solutions}}<li>{{.}}</li>{{end}}
                </ul>
            </div>
        </div>
        {{end}}
    </div>
    {{end}}
</body>
</html>`

	// Markdown catalog template
	markdownCatalogTmpl := `# {{.Title}}

Generated: {{.Generated}}
Total Errors: {{.TotalErrors}}

{{range $category, $errors := .Categories}}
## {{$category}}

{{range $errors}}
### {{.ErrorCode}} - {{.Name}}

**Description:** {{.Description}}

**Severity:** {{.Severity}}

**Common Causes:**
{{range .Causes}}
- {{.}}
{{end}}

**Solutions:**
{{range .Solutions}}
- {{.}}
{{end}}

{{if .BestPractices}}
**Best Practices:**
{{range .BestPractices}}
- {{.}}
{{end}}
{{end}}

---

{{end}}
{{end}}`

	// Troubleshooting guide template
	troubleshootingTmpl := `# Troubleshooting Guide: {{.Error.Name}}

**Error Code:** {{.Error.ErrorCode}}
**Generated:** {{.Generated}}

## Description
{{.Error.Description}}

## Common Causes
{{range .Error.Causes}}
- {{.}}
{{end}}

## Troubleshooting Steps
{{range .Error.Troubleshooting}}
### Step {{.Step}}: {{.Description}}

{{if .Command}}
**Command to run:**
` + "```" + `
{{.Command}}
` + "```" + `
{{end}}

{{if .Expected}}
**Expected result:** {{.Expected}}
{{end}}

{{if .IfFails}}
**If this fails:** {{.IfFails}}
{{end}}

{{end}}

## Solutions
{{range .Error.Solutions}}
- {{.}}
{{end}}

{{if .Error.BestPractices}}
## Best Practices
{{range .Error.BestPractices}}
- {{.}}
{{end}}
{{end}}

{{if .Error.References}}
## References
{{range .Error.References}}
- [{{.Title}}]({{.URL}}) - {{.Description}}
{{end}}
{{end}}`

	// Parse templates
	funcMap := template.FuncMap{
		"lower": strings.ToLower,
	}

	ec.templates["catalog"] = template.Must(template.New("catalog").Funcs(funcMap).Parse(htmlCatalogTmpl))
	ec.templates["markdown"] = template.Must(template.New("markdown").Parse(markdownCatalogTmpl))
	ec.templates["troubleshooting"] = template.Must(template.New("troubleshooting").Parse(troubleshootingTmpl))
}

// BestPracticesGuide provides best practices for error handling
type BestPracticesGuide struct {
	practices map[string]BestPractice
	mutex     sync.RWMutex
}

// BestPractice represents a best practice recommendation
type BestPractice struct {
	Title        string      `json:"title"`
	Description  string      `json:"description"`
	Category     string      `json:"category"`
	Priority     string      `json:"priority"` // "high", "medium", "low"
	Examples     []string    `json:"examples"`
	Antipatterns []string    `json:"antipatterns"`
	References   []Reference `json:"references"`
}

// NewBestPracticesGuide creates a new best practices guide
func NewBestPracticesGuide() *BestPracticesGuide {
	guide := &BestPracticesGuide{
		practices: make(map[string]BestPractice),
	}

	guide.initializeDefaultPractices()
	return guide
}

// AddBestPractice adds a best practice to the guide
func (bpg *BestPracticesGuide) AddBestPractice(id string, practice BestPractice) {
	bpg.mutex.Lock()
	defer bpg.mutex.Unlock()
	bpg.practices[id] = practice
}

// GetBestPractice retrieves a best practice by ID
func (bpg *BestPracticesGuide) GetBestPractice(id string) (BestPractice, bool) {
	bpg.mutex.RLock()
	defer bpg.mutex.RUnlock()
	practice, exists := bpg.practices[id]
	return practice, exists
}

// GetPracticesByCategory returns practices for a specific category
func (bpg *BestPracticesGuide) GetPracticesByCategory(category string) []BestPractice {
	bpg.mutex.RLock()
	defer bpg.mutex.RUnlock()

	var practices []BestPractice
	for _, practice := range bpg.practices {
		if practice.Category == category {
			practices = append(practices, practice)
		}
	}

	return practices
}

// initializeDefaultPractices sets up default best practices
func (bpg *BestPracticesGuide) initializeDefaultPractices() {
	practices := map[string]BestPractice{
		"error-wrapping": {
			Title:       "Proper Error Wrapping",
			Description: "Always wrap errors with context when passing them up the call stack",
			Category:    "General",
			Priority:    "high",
			Examples: []string{
				`return fmt.Errorf("failed to process user %s: %w", userID, err)`,
				`return errors.Wrap(err, "database operation failed")`,
			},
			Antipatterns: []string{
				`return err // Lost context`,
				`log.Println(err); return nil // Hidden error`,
			},
		},
		"circuit-breaker": {
			Title:       "Circuit Breaker Pattern",
			Description: "Use circuit breakers to prevent cascading failures",
			Category:    "Resilience",
			Priority:    "high",
			Examples: []string{
				`cb := NewCircuitBreaker(config)`,
				`return cb.Execute(func() error { return riskyOperation() })`,
			},
		},
		"timeout-handling": {
			Title:       "Timeout Handling",
			Description: "Always set appropriate timeouts for operations",
			Category:    "Performance",
			Priority:    "medium",
			Examples: []string{
				`ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)`,
				`defer cancel()`,
			},
		},
	}

	for id, practice := range practices {
		bpg.practices[id] = practice
	}
}

// Global error catalog instance
var globalCatalog = NewErrorCatalog()
var globalBestPractices = NewBestPracticesGuide()

// GetErrorDocumentation retrieves error documentation using the global catalog
func GetErrorDocumentation(errorCode string) (*ErrorDocumentation, bool) {
	return globalCatalog.GetErrorDocumentation(errorCode)
}

// SearchErrorDocumentation searches for error documentation using the global catalog
func SearchErrorDocumentation(query string) []*ErrorDocumentation {
	return globalCatalog.SearchDocumentation(query)
}

// GenerateHTMLErrorCatalog generates HTML catalog using the global catalog
func GenerateHTMLErrorCatalog(writer io.Writer) error {
	return globalCatalog.GenerateHTMLCatalog(writer)
}

// GenerateMarkdownErrorCatalog generates Markdown catalog using the global catalog
func GenerateMarkdownErrorCatalog(writer io.Writer) error {
	return globalCatalog.GenerateMarkdownCatalog(writer)
}

// GetBestPractices retrieves best practices using the global guide
func GetBestPractices(category string) []BestPractice {
	return globalBestPractices.GetPracticesByCategory(category)
}
