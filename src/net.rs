use std::net::{IpAddr, Ipv4Addr};

pub fn is_forbidden_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => forbidden_v4(ip),
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return forbidden_v4(mapped);
            }
            let segments = ip.segments();
            let global_unicast = segments[0] & 0xe000 == 0x2000;
            let documentation =
                segments[0] == 0x2001 && ((segments[1] == 0x0db8) || segments[1] & 0xfe00 == 0);
            ip.is_unspecified()
                || ip.is_loopback()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || ip.is_multicast()
                || !global_unicast
                || documentation
                || segments[0] == 0x2002 // 6to4 embeds an unchecked IPv4 destination.
                || (segments[0] == 0x3fff && segments[1] & 0xfff0 == 0)
        }
    }
}

fn forbidden_v4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    let shared_address_space = octets[0] == 100 && (64..=127).contains(&octets[1]);
    let protocol_assignments = octets[0] == 192 && octets[1] == 0 && octets[2] == 0;
    let documentation = (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
        || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
        || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113);
    let benchmark = octets[0] == 198 && (18..=19).contains(&octets[1]);
    let old_relay = octets[0] == 192 && octets[1] == 88 && octets[2] == 99;

    ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
        || octets[0] == 0
        || shared_address_space
        || protocol_assignments
        || documentation
        || benchmark
        || old_relay
        || octets[0] >= 240
}

#[cfg(test)]
mod tests {
    use super::is_forbidden_ip;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn rejects_non_public_ipv4_and_ipv6_ranges() {
        let forbidden = [
            "0.1.2.3",
            "10.1.2.3",
            "100.64.0.1",
            "100.127.255.254",
            "127.0.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "172.31.255.254",
            "192.168.1.1",
            "224.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "fd00::1",
            "fd00:ec2::254",
            "fe80::1",
            "ff02::1",
            "::ffff:127.0.0.1",
        ];
        for address in forbidden {
            let ip = address.parse::<IpAddr>().unwrap();
            assert!(is_forbidden_ip(ip), "{address} must be blocked");
        }
    }

    #[test]
    fn permits_public_ipv4_and_ipv6_addresses() {
        for ip in [
            IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
            IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
            IpAddr::V6("2606:4700:4700::1111".parse::<Ipv6Addr>().unwrap()),
        ] {
            assert!(!is_forbidden_ip(ip), "{ip} should be allowed");
        }
    }
}
