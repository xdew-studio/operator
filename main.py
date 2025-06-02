import kopf
import kubernetes
import logging
import yaml
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class XDEWOperator:
    def __init__(self):
        try:
            kubernetes.config.load_incluster_config()
            logger.info("Using in-cluster configuration")
        except kubernetes.config.ConfigException:
            try:
                kubernetes.config.load_kube_config()
                logger.info("Using local kubeconfig")
            except kubernetes.config.ConfigException:
                logger.error("Could not configure kubernetes client")
                raise
        
        self.v1 = kubernetes.client.CoreV1Api()
        self.rbac_v1 = kubernetes.client.RbacAuthorizationV1Api()
        self.custom_api = kubernetes.client.CustomObjectsApi()
        
    def get_default_resource_quota(self):
        return {
            "cpu": "2",
            "memory": "4Gi", 
            "storage": "10Gi",
            "pods": "10"
        }

    def generate_kubernetes_namespace_name(self, project_id: str, namespace_id: str) -> str:
        hash_input = f"{project_id}-{namespace_id}"
        hash_suffix = hashlib.md5(hash_input.encode()).hexdigest()[:8]
        return f"xdew-{project_id}-{namespace_id}-{hash_suffix}"[:63].lower()

    def get_project_by_id(self, project_id: str) -> Optional[Dict[str, Any]]:
        try:
            project = self.custom_api.get_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="projects",
                name=project_id
            )
            return project
        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 404:
                logger.warning(f"Project {project_id} not found")
                return None
            logger.error(f"Failed to get project {project_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to get project {project_id}: {e}")
            return None

    def create_kubernetes_namespace(self, k8s_namespace_name: str, project_id: str, namespace_name: str, owner_ref: Dict[str, Any], project_info: Dict[str, Any]):
        namespace = kubernetes.client.V1Namespace(
            metadata=kubernetes.client.V1ObjectMeta(
                name=k8s_namespace_name,
                labels={
                    "xdew.ch/project-id": project_id,
                    "xdew.ch/namespace-name": namespace_name,
                    "xdew.ch/project-owner": project_info.get("spec", {}).get("userId", ""),
                    "managed-by": "xdew-operator"
                },
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            )
        )
        self.v1.create_namespace(namespace)
        logger.info(f"Created Kubernetes namespace: {k8s_namespace_name}")

    def create_resource_quota(self, k8s_namespace_name: str, quota_spec: Dict[str, str], owner_ref: Dict[str, Any]):
        resource_quota = kubernetes.client.V1ResourceQuota(
            metadata=kubernetes.client.V1ObjectMeta(
                name="xdew-quota",
                namespace=k8s_namespace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            spec=kubernetes.client.V1ResourceQuotaSpec(
                hard={
                    "requests.cpu": quota_spec["cpu"],
                    "requests.memory": quota_spec["memory"],
                    "requests.storage": quota_spec["storage"],
                    "pods": quota_spec["pods"]
                }
            )
        )
        self.v1.create_namespaced_resource_quota(k8s_namespace_name, resource_quota)
        logger.info(f"Created resource quota in namespace: {k8s_namespace_name}")

    def create_rbac_resources(self, k8s_namespace_name: str, project_id: str, namespace_id: str, owner_ref: Dict[str, Any]):
        role_name = f"xdew-{project_id}-admin"
        
        role = kubernetes.client.V1Role(
            metadata=kubernetes.client.V1ObjectMeta(
                name=role_name,
                namespace=k8s_namespace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            rules=[
                kubernetes.client.V1PolicyRule(
                    api_groups=[""],
                    resources=["*"],
                    verbs=["*"]
                ),
                kubernetes.client.V1PolicyRule(
                    api_groups=["apps"],
                    resources=["*"],
                    verbs=["*"]
                ),
                kubernetes.client.V1PolicyRule(
                    api_groups=["extensions"],
                    resources=["*"],
                    verbs=["*"]
                ),
                kubernetes.client.V1PolicyRule(
                    api_groups=["networking.k8s.io"],
                    resources=["*"],
                    verbs=["*"]
                )
            ]
        )
        self.rbac_v1.create_namespaced_role(k8s_namespace_name, role)

        role_binding_admin = kubernetes.client.V1RoleBinding(
            metadata=kubernetes.client.V1ObjectMeta(
                name=f"{role_name}-binding",
                namespace=k8s_namespace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            subjects=[
                kubernetes.client.V1Subject(
                    kind="Group",
                    name=f"xdew-project-{project_id}-admins",
                    api_group="rbac.authorization.k8s.io"
                )
            ],
            role_ref=kubernetes.client.V1RoleRef(
                kind="Role",
                name=role_name,
                api_group="rbac.authorization.k8s.io"
            )
        )
        self.rbac_v1.create_namespaced_role_binding(k8s_namespace_name, role_binding_admin)

        readonly_role_name = f"xdew-{project_id}-readonly"
        readonly_role = kubernetes.client.V1Role(
            metadata=kubernetes.client.V1ObjectMeta(
                name=readonly_role_name,
                namespace=k8s_namespace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            rules=[
                kubernetes.client.V1PolicyRule(
                    api_groups=[""],
                    resources=["*"],
                    verbs=["get", "list", "watch"]
                ),
                kubernetes.client.V1PolicyRule(
                    api_groups=["apps"],
                    resources=["*"],
                    verbs=["get", "list", "watch"]
                ),
                kubernetes.client.V1PolicyRule(
                    api_groups=["extensions"],
                    resources=["*"],
                    verbs=["get", "list", "watch"]
                ),
                kubernetes.client.V1PolicyRule(
                    api_groups=["networking.k8s.io"],
                    resources=["*"],
                    verbs=["get", "list", "watch"]
                )
            ]
        )
        self.rbac_v1.create_namespaced_role(k8s_namespace_name, readonly_role)

        role_binding_readonly = kubernetes.client.V1RoleBinding(
            metadata=kubernetes.client.V1ObjectMeta(
                name=f"{readonly_role_name}-binding",
                namespace=k8s_namespace_name,
                owner_references=[kubernetes.client.V1OwnerReference(**owner_ref)]
            ),
            subjects=[
                kubernetes.client.V1Subject(
                    kind="Group",
                    name=f"xdew-project-{project_id}-readonly",
                    api_group="rbac.authorization.k8s.io"
                )
            ],
            role_ref=kubernetes.client.V1RoleRef(
                kind="Role",
                name=readonly_role_name,
                api_group="rbac.authorization.k8s.io"
            )
        )
        self.rbac_v1.create_namespaced_role_binding(k8s_namespace_name, role_binding_readonly)

        logger.info(f"Created RBAC resources for project: {project_id}")

    def update_project_status(self, name: str, status_update: Dict[str, Any]):
        try:
            project = self.custom_api.get_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="projects",
                name=name
            )
            
            if "status" not in project:
                project["status"] = {}
            
            project["status"].update(status_update)
            project["status"]["lastUpdated"] = datetime.now(timezone.utc).isoformat()
            
            self.custom_api.patch_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="projects",
                name=name,
                body=project
            )
            logger.info(f"Updated project status: {name}")
        except Exception as e:
            logger.error(f"Failed to update project status: {e}")

    def update_namespace_status(self, name: str, status_update: Dict[str, Any]):
        try:
            namespace = self.custom_api.get_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="namespaces",
                name=name
            )
            
            if "status" not in namespace:
                namespace["status"] = {}
            
            namespace["status"].update(status_update)
            namespace["status"]["lastUpdated"] = datetime.now(timezone.utc).isoformat()
            
            self.custom_api.patch_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="namespaces",
                name=name,
                body=namespace
            )
            logger.info(f"Updated namespace status: {name}")
        except Exception as e:
            logger.error(f"Failed to update namespace status: {e}")

    def update_project_namespace_count(self, project_id: str):
        try:
            project = self.get_project_by_id(project_id)
            if not project:
                return
                
            project_name = project["metadata"]["name"]  # metadata.name is the project ID
            
            namespaces = self.custom_api.list_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="namespaces"
            )
            
            count = 0
            for ns in namespaces.get("items", []):
                if ns.get("spec", {}).get("projectId") == project_id:
                    count += 1
            
            if "status" not in project:
                project["status"] = {}
            
            project["status"]["namespacesCount"] = count
            project["status"]["lastUpdated"] = datetime.now(timezone.utc).isoformat()
            
            self.custom_api.patch_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="projects",
                name=project_name,
                body=project
            )
            
        except Exception as e:
            logger.error(f"Failed to update project namespace count: {e}")

operator = XDEWOperator()

@kopf.on.create('xdew.ch', 'v1', 'projects')
def create_project(spec, name, uid, patch, **kwargs):
    logger.info(f"Creating project: {name}")
    
    project_name = spec.get('name')
    user_id = spec.get('userId')
    description = spec.get('description', '')
    
    # Update status using patch
    patch.status['createdAt'] = datetime.now(timezone.utc).isoformat()
    patch.status['namespacesCount'] = 0
    
    logger.info(f"Project {name} created successfully")
    # Don't return anything to avoid status conflicts
    return None

@kopf.on.create('xdew.ch', 'v1', 'namespaces')
def create_namespace(spec, name, uid, patch, **kwargs):
    logger.info(f"Creating namespace: {name}")
    
    namespace_id = name  # metadata.name is the namespace ID
    display_name = spec.get('displayName')
    project_id = spec.get('projectId')
    description = spec.get('description', '')
    resource_quota = spec.get('resourceQuota', {})
    
    project_info = operator.get_project_by_id(project_id)
    if not project_info:
        patch.status['state'] = "rejected"
        patch.status['message'] = f"Project {project_id} not found"
        patch.status['createdAt'] = datetime.now(timezone.utc).isoformat()
        logger.warning(f"Project {project_id} not found for namespace {name}")
        return None
    
    if not resource_quota:
        resource_quota = operator.get_default_resource_quota()
        
        # Update spec with default resource quota
        patch.spec['resourceQuota'] = resource_quota
        logger.info(f"Applied default resource quota to namespace: {name}")
    
    # Update status using patch
    patch.status['state'] = "requested"
    patch.status['message'] = "Namespace creation requested"
    patch.status['createdAt'] = datetime.now(timezone.utc).isoformat()
    
    operator.update_project_namespace_count(project_id)
    
    logger.info(f"Namespace {namespace_id} created in requested state")
    return None

@kopf.on.field('xdew.ch', 'v1', 'namespaces', field='status.state')
def handle_namespace_state_change(old, new, spec, name, uid, **kwargs):
    logger.info(f"Namespace {name} state changed from {old} to {new}")
    
    if new == "accepted":
        try:
            namespace_id = name  # metadata.name is the namespace ID
            display_name = spec.get('displayName')
            project_id = spec.get('projectId')
            resource_quota = spec.get('resourceQuota', operator.get_default_resource_quota())
            
            project_info = operator.get_project_by_id(project_id)
            if not project_info:
                status_update = {
                    "state": "rejected",
                    "message": f"Project {project_id} not found"
                }
                operator.update_namespace_status(name, status_update)
                return
            
            k8s_namespace_name = operator.generate_kubernetes_namespace_name(project_id, namespace_id)
            
            owner_ref = {
                "api_version": "xdew.ch/v1",
                "kind": "Namespace",
                "name": name,
                "uid": uid
            }
            
            operator.create_kubernetes_namespace(k8s_namespace_name, project_id, display_name, owner_ref, project_info)
            operator.create_resource_quota(k8s_namespace_name, resource_quota, owner_ref)
            operator.create_rbac_resources(k8s_namespace_name, project_id, namespace_id, owner_ref)
            
            status_update = {
                "state": "accepted",
                "message": "Namespace accepted and resources created",
                "kubernetesNamespace": k8s_namespace_name
            }
            operator.update_namespace_status(name, status_update)
            
        except Exception as e:
            logger.error(f"Failed to create resources for namespace {name}: {e}")
            status_update = {
                "state": "rejected",
                "message": f"Failed to create resources: {str(e)}"
            }
            operator.update_namespace_status(name, status_update)
    
    elif new == "deleted":
        try:
            namespace = operator.custom_api.get_cluster_custom_object(
                group="xdew.ch",
                version="v1",
                plural="namespaces",
                name=name
            )
            
            k8s_namespace_name = namespace.get("status", {}).get("kubernetesNamespace")
            if k8s_namespace_name:
                try:
                    operator.v1.delete_namespace(k8s_namespace_name)
                    logger.info(f"Deleted Kubernetes namespace: {k8s_namespace_name}")
                except kubernetes.client.exceptions.ApiException as e:
                    if e.status != 404:
                        raise
            
        except Exception as e:
            logger.error(f"Failed to delete resources for namespace {name}: {e}")

@kopf.on.delete('xdew.ch', 'v1', 'namespaces')
def delete_namespace(spec, name, patch, **kwargs):
    logger.info(f"Deleting namespace: {name}")
    
    project_id = spec.get('projectId')
    
    patch.status['state'] = "deleted"
    patch.status['message'] = "Namespace deletion in progress"
    
    operator.update_project_namespace_count(project_id)

@kopf.on.delete('xdew.ch', 'v1', 'projects')
def delete_project(spec, name, **kwargs):
    logger.info(f"Deleting project: {name}")
    
    project_id = name  # metadata.name is the project ID
    
    namespaces = operator.custom_api.list_cluster_custom_object(
        group="xdew.ch",
        version="v1",
        plural="namespaces"
    )
    
    for ns in namespaces.get("items", []):
        if ns.get("spec", {}).get("projectId") == project_id:
            ns_name = ns["metadata"]["name"]
            logger.info(f"Deleting associated namespace: {ns_name}")
            try:
                operator.custom_api.delete_cluster_custom_object(
                    group="xdew.ch",
                    version="v1",
                    plural="namespaces",
                    name=ns_name
                )
            except Exception as e:
                logger.error(f"Failed to delete namespace {ns_name}: {e}")

if __name__ == "__main__":
    kopf.configure(verbose=True)
    kopf.run()
