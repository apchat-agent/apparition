use std::net::IpAddr;

pub fn is_forbidden_ip(_ip: IpAddr) -> bool {
    false
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
