package account

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/teslamotors/vehicle-command/internal/authentication"
	"github.com/teslamotors/vehicle-command/internal/log"
	"github.com/teslamotors/vehicle-command/pkg/cache"
	"github.com/teslamotors/vehicle-command/pkg/connector/inet"
	"github.com/teslamotors/vehicle-command/pkg/connector/owner"
	"github.com/teslamotors/vehicle-command/pkg/vehicle"
)

const defaultOwnerDomain = "https://owner-api.teslamotors.com/"

func (p *oauthPayload) ownerDomain() string {
	if len(remappedDomains) > 0 {
		for _, a := range p.Audiences {
			if d, ok := remappedDomains[a]; ok {
				return d
			}
		}
	}
	domain := defaultOwnerDomain
	ouCodeMatch := fmt.Sprintf(".%s.", strings.ToLower(p.OUCode))
	for _, u := range p.Audiences {
		if strings.HasPrefix(u, "https://auth.tesla.") {
			continue
		}
		d, _ := strings.CutPrefix(u, "https://")
		d, _ = strings.CutSuffix(d, "/")
		if !domainRegEx.MatchString(d) {
			continue
		}

		if inet.ValidTeslaDomainSuffix(d) && strings.HasPrefix(d, "owner-api.") {
			domain = d
			// Prefer domains that contain the ou_code (region)
			if strings.Contains(domain, ouCodeMatch) {
				return domain
			}
		}
	}
	return domain
}

func NewOwner(oauthToken, userAgent string) (*Account, error) {
	parts := strings.Split(oauthToken, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("client provided malformed OAuth token")
	}
	payloadJSON, err := base64.RawStdEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("client provided malformed OAuth token: %s (%s)", err, parts[1])
	}
	var payload oauthPayload
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		return nil, fmt.Errorf("client provided malformed OAuth token: %s", err)
	}

	domain := payload.ownerDomain()
	if domain == "" {
		return nil, fmt.Errorf("client provided OAuth token with invalid audiences")
	}
	return &Account{
		UserAgent:  buildUserAgent(userAgent),
		authHeader: "Bearer " + strings.TrimSpace(oauthToken),
		Host:       domain,
	}, nil
}

func (a *Account) GetVehicleHermes(ctx context.Context, vin string, privateKey authentication.ECDHPrivateKey, sessions *cache.SessionCache) (*vehicle.Vehicle, error) {
	if a.hermesUserToken == "" {
		hermesUserToken, err := a.fetchHermesToken("users/jwt/hermes", a.authHeader)
		if err != nil {
			return nil, err
		}
		a.hermesUserToken = hermesUserToken
	}

	if a.hermesVehicleToken == "" {
		hermesVehicleToken, err := a.fetchHermesToken("vehicles/"+vin+"/jwt/hermes", a.authHeader)
		if err != nil {
			return nil, err
		}
		a.hermesVehicleToken = hermesVehicleToken
	}

	conn, err := owner.NewConnection(vin, a.hermesUserToken, a.hermesVehicleToken)
	if err != nil {
		return nil, err
	}
	car, err := vehicle.NewVehicle(conn, privateKey, sessions)
	if err != nil {
		conn.Close()
	}
	return car, err
}

func (a *Account) fetchHermesToken(path, authHeader string) (string, error) {
	url := fmt.Sprintf("https://%s/api/1/%s", a.Host, path)
	uuid := fmt.Sprintf("{\"uuid\": \"%s\" }", uuid.New())
	req, _ := http.NewRequest("POST", url, bytes.NewBufferString(uuid))

	req.Header.Add("Authorization", authHeader)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", a.UserAgent)
	log.Debug("Fetching Token from %s", url)
	client := &http.Client{}
	resp, err := client.Do(req)

	var response struct {
		Token string `json:"token"`
	}

	body, _ := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	err = json.Unmarshal(body, &response)
	if err != nil {
		return "", err
	}

	return response.Token, nil
}
