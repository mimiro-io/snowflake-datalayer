package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/mimiro-io/internal-go-util/pkg/uda"
)

type Entity struct {
	ID         string         `json:"id"`
	Recorded   uint64         `json:"recorded"`
	IsDeleted  bool           `json:"deleted"`
	Dataset    string         `json:"-"`
	References map[string]any `json:"refs"`
	Properties map[string]any `json:"props"`
}

func AsContext(value *Entity) *uda.Context {
	ctx := &uda.Context{
		ID:              value.ID,
		Namespaces:      map[string]string{},
		NamespaceLookup: map[string]string{},
		RDFs:            map[string]string{},
	}
	namespaces, ok := value.Properties["namespaces"]
	if ok {
		ns := make(map[string]string)
		for k, v := range namespaces.(map[string]any) {
			ns[k] = v.(string)
		}

		ctx.Namespaces = ns

		// make a lookup
		lookup := make(map[string]string)
		rdfs := make(map[string]string)
		for k, v := range ctx.Namespaces {
			lookup[v] = k
			rdfs[k] = namespace(v)
		}
		ctx.NamespaceLookup = lookup
		ctx.RDFs = rdfs
	}
	return ctx
}

func namespace(ns string) string {
	// clean up a bit
	clean := strings.ReplaceAll(ns, "/", " ")
	clean = strings.TrimSpace(clean)
	parts := strings.Fields(clean)
	return parts[len(parts)-1]
}

// NewEntity Create a new entity with global uri and internal resource id
func NewEntity(ID string) *Entity {
	e := Entity{}
	e.ID = ID
	e.Properties = make(map[string]any)
	e.References = make(map[string]any)
	return &e
}

type EntityStreamParser struct {
	localNamespaces       map[string]string
	localPropertyMappings map[string]string
	processingContext     bool
}

func NewEntityStreamParser() *EntityStreamParser {
	esp := &EntityStreamParser{}
	esp.localNamespaces = make(map[string]string)
	esp.localPropertyMappings = make(map[string]string)
	return esp
}

func (esp *EntityStreamParser) ParseStream(reader io.Reader, emitEntity func(*Entity) error) error {

	decoder := json.NewDecoder(reader)

	// expect Start of array
	t, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("parsing error: Bad token at Start of stream: %w", err)
	}

	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return errors.New("parsing error: Expected [ at Start of document")
	}

	// decode context object
	context := make(map[string]any)
	err = decoder.Decode(&context)
	if err != nil {
		return fmt.Errorf("parsing error: Unable to decode context: %w", err)
	}

	if context["id"] == "@context" {
		for k, v := range context["namespaces"].(map[string]any) {
			esp.localNamespaces[k] = v.(string)
		}
	} else {
		return errors.New("first entity in array must be a context")
	}

	contextEntity := NewEntity("@context")
	contextEntity.Properties = context
	_ = emitEntity(contextEntity)

	for {
		t, err = decoder.Token()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return fmt.Errorf("parsing error: Unable to read next token: %w", err)
			}
		}

		switch v := t.(type) {
		case json.Delim:
			if v == '{' {
				e, err := esp.parseEntity(decoder)
				if err != nil {
					return fmt.Errorf("parsing error: Unable to parse entity: %w", err)
				}
				err = emitEntity(e)
				if err != nil {
					return err
				}
			} else if v == ']' {
				// done
				break
			}
		default:
			return errors.New("parsing error: unexpected value in entity array")
		}
	}

	return nil
}

func (esp *EntityStreamParser) parseEntity(decoder *json.Decoder) (*Entity, error) {
	e := &Entity{}
	e.Properties = make(map[string]any)
	e.References = make(map[string]any)
	isContinuation := false
	for {
		t, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("unable to read token: %w", err)
		}

		switch v := t.(type) {
		case json.Delim:
			if v == '}' {
				return e, nil
			}
		case string:
			if v == "id" {
				val, err := decoder.Token()
				if err != nil {
					return nil, fmt.Errorf("unable to read token of id value %w", err)
				}

				if val.(string) == "@continuation" {
					e.ID = "@continuation"
					isContinuation = true
				} else {
					nsId, err := esp.getNamespacedIdentifier(val.(string), esp.localNamespaces)
					if err != nil {
						return nil, err
					}
					e.ID = nsId
				}
			} else if v == "recorded" {
				val, err := decoder.Token()
				if err != nil {
					return nil, fmt.Errorf("unable to read token of recorded value: %w", err)
				}
				e.Recorded = uint64(val.(float64))

			} else if v == "deleted" {
				val, err := decoder.Token()
				if err != nil {
					return nil, fmt.Errorf("unable to read token of deleted value: %w", err)
				}
				e.IsDeleted = val.(bool)

			} else if v == "props" {
				e.Properties, err = esp.parseProperties(decoder)
				if err != nil {
					return nil, fmt.Errorf("unable to parse properties: %w", err)
				}
			} else if v == "refs" {
				e.References, err = esp.parseReferences(decoder)
				if err != nil {
					return nil, fmt.Errorf("unable to parse references: %w", err)
				}
			} else if v == "token" {
				if !isContinuation {
					return nil, errors.New("token property found but not a continuation entity")
				}
				val, err := decoder.Token()
				if err != nil {
					return nil, fmt.Errorf("unable to read continuation token value: %w", err)
				}
				e.Properties = make(map[string]any)
				e.Properties["token"] = val
			} else {
				// log named property
				// read value
				_, err := decoder.Token()
				if err != nil {
					return nil, fmt.Errorf("unable to parse value of unknown key: %s %w", v, err)
				}
			}
		default:
			return nil, errors.New("unexpected value in entity")
		}
	}
}

func (esp *EntityStreamParser) parseReferences(decoder *json.Decoder) (map[string]any, error) {
	refs := make(map[string]any)

	_, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("unable to read token of at Start of references: %w", err)
	}

	for {
		t, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("unable to read token in parse references: %w", err)
		}

		switch v := t.(type) {
		case json.Delim:
			if v == '}' {
				return refs, nil
			}
		case string:
			val, err := esp.parseRefValue(decoder)
			if err != nil {
				return nil, errors.New("unable to parse value of reference key " + v)
			}

			propName := esp.localPropertyMappings[v]
			if propName == "" {
				propName, err = esp.getNamespacedIdentifier(v, esp.localNamespaces)
				if err != nil {
					return nil, err
				}
				esp.localPropertyMappings[v] = propName
			}
			refs[propName] = val
		default:
			return nil, errors.New("unknown type")
		}
	}
}

func (esp *EntityStreamParser) parseProperties(decoder *json.Decoder) (map[string]any, error) {
	props := make(map[string]interface{})

	_, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("unable to read token of at Start of properties: %w", err)
	}

	for {
		t, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("unable to read token in parse properties: %w", err)
		}

		switch v := t.(type) {
		case json.Delim:
			if v == '}' {
				return props, nil
			}
		case string:
			val, err := esp.parseValue(decoder)
			if err != nil {
				return nil, fmt.Errorf("unable to parse property value of key %s err: %w", v, err)
			}

			if val != nil { // basically if both error is nil, and value is nil, we drop the field
				propName := esp.localPropertyMappings[v]
				if propName == "" {
					propName, err = esp.getNamespacedIdentifier(v, esp.localNamespaces)
					if err != nil {
						return nil, err
					}
					esp.localPropertyMappings[v] = propName
				}
				props[propName] = val
			}
		default:
			return nil, errors.New("unknown type")
		}
	}
}

func (esp *EntityStreamParser) parseRefValue(decoder *json.Decoder) (any, error) {
	for {
		t, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("unable to read token in parse value: %w", err)
		}

		switch v := t.(type) {
		case json.Delim:
			if v == '[' {
				return esp.parseRefArray(decoder)
			}
		case string:
			nsRef, err := esp.getNamespacedIdentifier(v, esp.localNamespaces)
			if err != nil {
				return nil, err
			}
			return nsRef, nil
		default:
			return nil, errors.New("unknown token in parse ref value")
		}
	}
}

func (esp *EntityStreamParser) parseRefArray(decoder *json.Decoder) ([]string, error) {
	array := make([]string, 0)
	for {
		t, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("unable to read token in parse ref array: %w", err)
		}

		switch v := t.(type) {
		case json.Delim:
			if v == ']' {
				return array, nil
			}
		case string:
			nsRef, err := esp.getNamespacedIdentifier(v, esp.localNamespaces)
			if err != nil {
				return nil, err
			}
			array = append(array, nsRef)
		default:
			return nil, errors.New("unknown type")
		}
	}
}

func (esp *EntityStreamParser) parseArray(decoder *json.Decoder) ([]any, error) {
	array := make([]interface{}, 0)
	for {
		t, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("unable to read token in parse array: %w", err)
		}

		switch v := t.(type) {
		case json.Delim:
			if v == '{' {
				r, err := esp.parseEntity(decoder)
				if err != nil {
					return nil, fmt.Errorf("unable to parse array: %w", err)
				}
				array = append(array, r)
			} else if v == ']' {
				return array, nil
			} else if v == '[' {
				r, err := esp.parseArray(decoder)
				if err != nil {
					return nil, fmt.Errorf("unable to parse array: %w", err)
				}
				array = append(array, r)
			}
		case string:
			array = append(array, v)
		case int:
			array = append(array, v)
		case float64:
			array = append(array, v)
		case bool:
			array = append(array, v)
		case nil:
			array = append(array, v)
		default:
			return nil, errors.New("unknown type")
		}
	}
}

func (esp *EntityStreamParser) parseValue(decoder *json.Decoder) (any, error) {
	for {
		t, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("unable to read token in parse value: %w", err)
		}

		if t == nil {
			// there is a good chance that we got a null value, and we need to handle that
			return nil, nil
		}

		switch v := t.(type) {
		case json.Delim:
			if v == '{' {
				return esp.parseEntity(decoder)
			} else if v == '[' {
				return esp.parseArray(decoder)
			}
		case string:
			return v, nil
		case int:
			return v, nil
		case float64:
			return v, nil
		case bool:
			return v, nil
		default:
			return nil, errors.New("unknown token in parse value")
		}
	}
}

func (esp *EntityStreamParser) getNamespacedIdentifier(val string, localNamespaces map[string]string) (string, error) {

	if val == "" {
		return "", errors.New("empty value not allowed")
	}

	if strings.HasPrefix(val, "http://") {
		expansion, lastPathPart, err := getUrlParts(val)
		if err != nil {
			return "", err
		}

		// check for global expansion
		prefix, err := esp.assertPrefixMappingForExpansion(expansion)
		if err != nil {
			return "", nil
		}
		return prefix + ":" + lastPathPart, nil
	}

	if strings.HasPrefix(val, "https://") {
		expansion, lastPathPart, err := getUrlParts(val)
		if err != nil {
			return "", err
		}

		// check for global expansion
		prefix, err := esp.assertPrefixMappingForExpansion(expansion)
		if err != nil {
			return "", err
		}
		return prefix + ":" + lastPathPart, nil
	}

	indexOfColon := strings.Index(val, ":")
	if indexOfColon == -1 {
		localExpansion := localNamespaces["_"]
		if localExpansion == "" {
			return "", errors.New("no expansion for default prefix _ ")
		}

		prefix, err := esp.assertPrefixMappingForExpansion(localExpansion)
		if err != nil {
			return "", err
		}
		return prefix + ":" + val, nil

	} else {
		return val, nil
	}
}

func (esp *EntityStreamParser) assertPrefixMappingForExpansion(uriExpansion string) (string, error) {

	prefix := esp.localNamespaces[uriExpansion]
	if prefix == "" {
		prefix = "ns" + strconv.Itoa(len(esp.localNamespaces))
		esp.localNamespaces[prefix] = uriExpansion
	}
	return prefix, nil
}

func getUrlParts(url string) (string, string, error) {

	index := strings.LastIndex(url, "#")
	if index > -1 {
		return url[:index+1], url[index+1:], nil
	}

	index = strings.LastIndex(url, "/")
	if index > -1 {
		return url[:index+1], url[index+1:], nil
	}

	return "", "", errors.New("unable to split url") // fixme do something better
}